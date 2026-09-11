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
        found = []  # (rel, full)
        for dirpath, _dirnames, filenames in os.walk(root):
            for fn in filenames:
                if fn.lower().endswith(".xlsx") and not fn.startswith("~$"):
                    full = os.path.join(dirpath, fn)
                    found.append((os.path.relpath(full, root), full))
        db.execute("UPDATE machines SET online=1, last_scan_at=?, last_error=NULL WHERE id=?",
                   (db.now_str(), self.machine["id"]))

        # 阶段1：登记新文件 / pending→stable / 检测指纹变化
        changed_batches = set()  # (category, batch_name)
        for rel, full in found:
            if self._register(rel, full, changed_batches) is None:
                continue

        # 阶段2：指纹变化的批次整体重算（清数据，文件回 stable）
        for category, batch_name in changed_batches:
            self._reset_batch(category, batch_name)

        # 阶段3：解析所有 stable 文件
        for rel, full in found:
            self._process_if_stable(rel, full)

    def _register(self, rel, full, changed_batches):
        """登记/推进文件状态。返回 None 表示无法归类。"""
        info = parser.split_rel_path(self.machine["name"], rel)
        if info is None:
            return None
        category, batch_name, kind = info
        st = os.stat(full)
        size, mtime = st.st_size, st.st_mtime

        row = db.query_one(
            "SELECT * FROM files WHERE machine_id=? AND rel_path=?",
            (self.machine["id"], rel))

        if row is None:
            # 新文件：mtime 距今很久 → 直接 stable，否则 pending 等下一轮确认
            status = "stable" if (time.time() - mtime) > STABLE_ASSUME_SEC else "pending"
            db.execute(
                "INSERT INTO files(machine_id,rel_path,kind,size,mtime,status,"
                "first_size,first_seen) VALUES(?,?,?,?,?,?,?,?)",
                (self.machine["id"], rel, kind, size, mtime, status, size, db.now_str()))
            return status

        if row["status"] == "pending":
            if size == row["first_size"]:
                db.execute("UPDATE files SET status='stable' WHERE id=?", (row["id"],))
            else:
                # 大小仍在变，继续等待
                db.execute("UPDATE files SET first_size=? WHERE id=?", (size, row["id"]))
        elif row["status"] == "done":
            if size != row["size"] or mtime != row["mtime"]:
                # 文件被重新上传/改写 → 批次重算
                changed_batches.add((category, batch_name))
        # error 状态：文件指纹变化时才重试
        elif row["status"] == "error":
            if size != row["size"] or mtime != row["mtime"]:
                db.execute("UPDATE files SET status='stable' WHERE id=?", (row["id"],))
        return row["status"]

    def _reset_batch(self, category, batch_name):
        """批次数据整体重算：清空聚合/明细/告警，文件回 stable。"""
        batch = db.query_one(
            "SELECT id FROM batches WHERE machine_id=? AND category=? AND batch_name=?",
            (self.machine["id"], category, batch_name))
        if not batch:
            return
        bid = batch["id"]
        db.execute("DELETE FROM ng_details WHERE batch_id=?", (bid,))
        db.execute("DELETE FROM measure_stats WHERE batch_id=?", (bid,))
        db.execute("DELETE FROM alerts WHERE batch_id=?", (bid,))
        db.execute("UPDATE batches SET total=0, ok_count=0, ng_count=0, other_count=0, "
                   "first_time=NULL, last_time=NULL, updated_at=? WHERE id=?",
                   (db.now_str(), bid))
        # 该批次下所有文件（含 done/error）重置为 stable 重新解析
        db.execute("UPDATE files SET status='stable' WHERE batch_id=?", (bid,))

    # ---------------- 解析入库 ----------------

    def _process_if_stable(self, rel, full):
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
