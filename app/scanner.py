# -*- coding: utf-8 -*-
"""采集扫描器：每台机一个独立线程，轮询共享目录。

核心机制：
1. 增量去重：以 (机台, 相对路径) 唯一，files 表记录状态
2. 传输完整性：新文件先 pending，下一轮扫描大小不变才 stable；
   mtime 距今超过 10 分钟的显然不是传输中文件，直接 stable
3. 断点续传：状态落库，程序重启后继续
4. 批次重算：已 done 的文件指纹(size/mtime)变化（重新上传/机台改写）时，
   清空该批次全部数据重新解析，保证计数不重复
"""
import json
import os
import threading
import time
import traceback

from . import database as db
from . import parser
from . import rules

# mtime 距今超过该秒数，认为文件不可能仍在传输中
STABLE_ASSUME_SEC = 600


class MachineScanner(threading.Thread):
    def __init__(self, machine, interval):
        super().__init__(daemon=True, name=f"scanner-{machine['name']}")
        self.machine = machine
        self.interval = max(10, int(interval))
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            try:
                self.scan_once()
            except Exception as e:
                # 目录不可达等：标记离线，下轮继续重试
                db.execute("UPDATE machines SET online=0, last_error=? WHERE id=?",
                           (f"{e}", self.machine["id"]))
            self.stop_event.wait(self.interval)

    # ---------------- 扫描主流程 ----------------

    def scan_once(self):
        root = self.machine["path"]
        if not os.path.isdir(root):
            # 共享不可达：标记离线，等待网络恢复后自动重试
            db.execute("UPDATE machines SET online=0, last_error=? WHERE id=?",
                       ("报表目录不可达", self.machine["id"]))
            return

        # 1) walk 收集指纹（每文件一次 stat，不可避免的开销）
        found = {}  # rel_path -> (full, size, mtime)
        for dirpath, _dirnames, filenames in os.walk(root):
            for fn in filenames:
                if fn.lower().endswith(".xlsx") and not fn.startswith("~$"):
                    full = os.path.join(dirpath, fn)
                    try:
                        st = os.stat(full)
                    except OSError:
                        continue  # 扫描瞬间文件消失（正在重命名等），下轮再看
                    found[os.path.relpath(full, root)] = (full, st.st_size, st.st_mtime)
        db.execute("UPDATE machines SET online=1, last_scan_at=?, last_error=NULL WHERE id=?",
                   (db.now_str(), self.machine["id"]))

        # 2) 一次性读出该机台全部文件指纹，内存比对差量
        #    （消除"每文件一次 SELECT"的库往返，文件量上万时的主要开销）
        db_files = {r["rel_path"]: r for r in db.query(
            "SELECT id, rel_path, kind, size, mtime, status, first_size FROM files "
            "WHERE machine_id=?", (self.machine["id"],))}

        inserts = []          # 新文件
        to_stable = []        # pending -> stable 的文件 id
        to_refirst = []       # pending 且大小仍变化 -> 刷新 first_size (id, size)
        stable_rels = []      # 本轮需要解析的 (rel, full)
        changed_batches = set()

        for rel, (full, size, mtime) in found.items():
            info = parser.split_rel_path(self.machine["name"], rel)
            if info is None:
                continue
            category, batch_name, kind = info
            row = db_files.get(rel)

            if row is None:
                # 新文件：mtime 距今很久 → 显然非传输中，直接 stable
                status = "stable" if (time.time() - mtime) > STABLE_ASSUME_SEC else "pending"
                inserts.append((self.machine["id"], rel, kind, size, mtime, status,
                                size, db.now_str()))
                if status == "stable":
                    stable_rels.append((rel, full))
            elif row["status"] == "pending":
                if size == row["first_size"]:
                    to_stable.append(row["id"])
                    stable_rels.append((rel, full))
                else:
                    to_refirst.append((size, row["id"]))  # 大小仍在变，继续等待
            elif row["status"] == "stable":
                stable_rels.append((rel, full))
            else:  # done / error：指纹变化才重做
                if size != row["size"] or mtime != row["mtime"]:
                    changed_batches.add((category, batch_name))

        # 3) 差量批量落库
        if inserts:
            db.execute_many(
                "INSERT OR IGNORE INTO files(machine_id,rel_path,kind,size,mtime,status,"
                "first_size,first_seen) VALUES(?,?,?,?,?,?,?,?)", inserts)
        if to_stable:
            db.execute_many("UPDATE files SET status='stable' WHERE id=?",
                             [(i,) for i in to_stable])
        if to_refirst:
            db.execute_many("UPDATE files SET first_size=? WHERE id=?", to_refirst)

        # 4) 指纹变化的批次整体重算（清数据，文件回 stable，随本轮重新解析）
        for category, batch_name in changed_batches:
            stable_rels.extend(self._reset_batch(category, batch_name))

        # 5) 解析所有 stable 文件
        for rel, full in stable_rels:
            self._process_stable(rel, full)

    def _reset_batch(self, category, batch_name):
        """批次数据整体重算：清空聚合/明细/告警，文件回 stable。
        返回重置后的 [(rel, full)] 供本轮继续解析。"""
        batch = db.query_one(
            "SELECT id FROM batches WHERE machine_id=? AND category=? AND batch_name=?",
            (self.machine["id"], category, batch_name))
        if not batch:
            return []
        bid = batch["id"]
        db.execute("DELETE FROM ng_details WHERE batch_id=?", (bid,))
        db.execute("DELETE FROM measure_stats WHERE batch_id=?", (bid,))
        db.execute("DELETE FROM alerts WHERE batch_id=?", (bid,))
        db.execute("UPDATE batches SET total=0, ok_count=0, ng_count=0, other_count=0, "
                   "first_time=NULL, last_time=NULL, updated_at=? WHERE id=?",
                   (db.now_str(), bid))
        rows = db.query("SELECT rel_path FROM files WHERE batch_id=?", (bid,))
        db.execute("UPDATE files SET status='stable' WHERE batch_id=?", (bid,))
        return [(r["rel_path"], os.path.join(self.machine["path"], r["rel_path"]))
                for r in rows]

    # ---------------- 解析入库 ----------------

    def _process_stable(self, rel, full):
        row = db.query_one(
            "SELECT * FROM files WHERE machine_id=? AND rel_path=?",
            (self.machine["id"], rel))
        if not row or row["status"] != "stable":
            return
        info = parser.split_rel_path(self.machine["name"], rel)
        if info is None:
            return
        category, batch_name, kind = info

        batch_id = self._ensure_batch(category, batch_name)
        db.execute("UPDATE files SET batch_id=? WHERE machine_id=? AND rel_path=?",
                   (batch_id, self.machine["id"], rel))

        # 网络共享路径：先整块复制到本地再解析（随机读 SMB 慢一个数量级）
        local = full
        tmp = None
        if full.startswith("\\\\"):
            import shutil
            import tempfile
            fd, tmp = tempfile.mkstemp(suffix=".xlsx",
                                       dir=os.path.join(os.path.dirname(
                                           os.path.dirname(os.path.abspath(__file__))),
                                           "data"))
            os.close(fd)
            shutil.copyfile(full, tmp)
            local = tmp

        try:
            if kind == "defect":
                self._ingest_defect(batch_id, local, rel)
            elif kind == "measure":
                self._ingest_measure(batch_id, parser.parse_measure_file(local))
            elif kind == "measure_3d":
                self._ingest_measure(batch_id, parser.parse_3d_file(local))
            db.execute("UPDATE files SET status='done', parsed_at=? WHERE id=?",
                       (db.now_str(), row["id"]))
        except Exception:
            db.execute("UPDATE files SET status='error', error=? WHERE id=?",
                       (traceback.format_exc()[-2000:], row["id"]))
            if tmp:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
            return
        finally:
            if tmp and os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass

        # 数据更新后立刻评估该批次规则（引擎异常不能拖垮采集循环）
        try:
            rules.evaluate_batch(batch_id)
        except Exception:
            import sys
            print("[rules] evaluate_batch error:", traceback.format_exc(), file=sys.stderr)

    def _ensure_batch(self, category, batch_name):
        row = db.query_one(
            "SELECT id FROM batches WHERE machine_id=? AND category=? AND batch_name=?",
            (self.machine["id"], category, batch_name))
        if row:
            return row["id"]
        return db.execute(
            "INSERT INTO batches(machine_id,category,batch_name,updated_at)"
            " VALUES(?,?,?,?)",
            (self.machine["id"], category, batch_name, db.now_str()))

    def _ingest_defect(self, batch_id, full, rel):
        d = parser.parse_defect_file(full)
        times = [t for _, t, _ in d["rows"] if t]
        first = min(times) if times else None
        last = max(times) if times else None
        db.execute(
            "UPDATE batches SET total=total+?, ok_count=ok_count+?, ng_count=ng_count+?, "
            "other_count=other_count+?, "
            "first_time=CASE WHEN first_time IS NULL OR ?<first_time THEN ? ELSE first_time END, "
            "last_time=CASE WHEN last_time IS NULL OR ?>last_time THEN ? ELSE last_time END, "
            "updated_at=? WHERE id=?",
            (d["total"], d["ok"], d["ng"], d["other"], first, first, last, last,
             db.now_str(), batch_id))
        # 相机级（文件级）计数，用于一致性校验
        db.execute(
            "UPDATE files SET ok_count=?, ng_count=?, other_count=?, total_count=? "
            "WHERE machine_id=? AND rel_path=?",
            (d["ok"], d["ng"], d["other"], d["total"],
             self.machine["id"], rel))
        if d["defects"]:
            db.execute_many(
                "INSERT INTO ng_details(batch_id,record_id,time,defects) VALUES(?,?,?,?)",
                [(batch_id, rid, t, json.dumps(defects, ensure_ascii=False))
                 for rid, t, defects in d["defects"]])

    def _ingest_measure(self, batch_id, stats):
        # 一次读出该批次现有统计，内存合并后全部批量写回（避免逐指标事务）
        existing = {r["metric_name"]: r for r in db.query(
            "SELECT * FROM measure_stats WHERE batch_id=?", (batch_id,))}
        inserts, updates = [], []
        for metric, values in stats.items():
            if not values:
                continue
            r = existing.get(metric)
            if r:
                updates.append((r["cnt"] + len(values), r["sum_v"] + sum(values),
                                min(r["min_v"], min(values)), max(r["max_v"], max(values)),
                                r["id"]))
            else:
                inserts.append((batch_id, metric, len(values), sum(values),
                                min(values), max(values)))
        if inserts:
            db.execute_many(
                "INSERT INTO measure_stats(batch_id,metric_name,cnt,sum_v,min_v,max_v)"
                " VALUES(?,?,?,?,?,?)", inserts)
        if updates:
            db.execute_many(
                "UPDATE measure_stats SET cnt=?, sum_v=?, min_v=?, max_v=? WHERE id=?",
                updates)
        db.execute("UPDATE batches SET updated_at=? WHERE id=?",
                   (db.now_str(), batch_id))


# ---------------- 扫描线程管理 ----------------

_threads = {}
_threads_lock = threading.Lock()


def start_scanners(interval):
    """为所有启用的机台各启动一个扫描线程（已启动的跳过）"""
    for m in db.query("SELECT * FROM machines WHERE enabled=1"):
        with _threads_lock:
            if m["id"] not in _threads:
                t = MachineScanner(m, interval)
                _threads[m["id"]] = t
                t.start()


def start_machine(machine_id, interval):
    """新增/启动机台时立即接入扫描"""
    m = db.query_one("SELECT * FROM machines WHERE id=?", (machine_id,))
    if not m or not m["enabled"]:
        return
    with _threads_lock:
        if machine_id not in _threads:
            t = MachineScanner(m, interval)
            _threads[machine_id] = t
            t.start()


def restart_machine(machine_id, interval):
    """机台信息（路径等）变更后重启其扫描线程，立即生效"""
    with _threads_lock:
        t = _threads.pop(machine_id, None)
        if t:
            t.stop()
    start_machine(machine_id, interval)
