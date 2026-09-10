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

    def run(self):
        while True:
            try:
                self.scan_once()
            except Exception as e:
                # 目录不可达等：标记离线，下轮继续重试
                db.execute("UPDATE machines SET online=0, last_error=? WHERE id=?",
                           (f"{e}", self.machine["id"]))
            time.sleep(self.interval)

    # ---------------- 扫描主流程 ----------------

    def scan_once(self):
        root = self.machine["path"]
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
        db.execute("UPDATE batches SET total=0, ok_count=0, ng_count=0, "
                   "first_time=NULL, last_time=NULL, updated_at=? WHERE id=?",
                   (db.now_str(), bid))
        # 该批次下所有文件（含 done/error）重置为 stable 重新解析
        db.execute(
            "UPDATE files SET status='stable' WHERE machine_id=? AND ("
            " rel_path LIKE ? OR rel_path LIKE ? )",
            (self.machine["id"],
             f"%/{category}%{batch_name}%", f"%\\{category}%{batch_name}%"))

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

        try:
            if kind == "defect":
                self._ingest_defect(batch_id, full)
            elif kind == "measure":
                self._ingest_measure(batch_id, parser.parse_measure_file(full))
            elif kind == "measure_3d":
                self._ingest_measure(batch_id, parser.parse_3d_file(full))
            db.execute("UPDATE files SET status='done', parsed_at=? WHERE id=?",
                       (db.now_str(), row["id"]))
        except Exception:
            db.execute("UPDATE files SET status='error', error=? WHERE id=?",
                       (traceback.format_exc()[-2000:], row["id"]))
            return

        # 数据更新后立刻评估该批次规则
        rules.evaluate_batch(batch_id)

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

    def _ingest_defect(self, batch_id, full):
        d = parser.parse_defect_file(full)
        batch = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
        times = [t for _, t, _ in d["rows"] if t]
        first = min(times) if times else None
        last = max(times) if times else None
        db.execute(
            "UPDATE batches SET total=total+?, ok_count=ok_count+?, ng_count=ng_count+?, "
            "first_time=CASE WHEN first_time IS NULL OR ?<first_time THEN ? ELSE first_time END, "
            "last_time=CASE WHEN last_time IS NULL OR ?>last_time THEN ? ELSE last_time END, "
            "updated_at=? WHERE id=?",
            (d["total"], d["ok"], d["ng"], first, first, last, last,
             db.now_str(), batch_id))
        for rid, t, defects in d["defects"]:
            db.execute(
                "INSERT INTO ng_details(batch_id,record_id,time,defects) VALUES(?,?,?,?)",
                (batch_id, rid, t, json.dumps(defects, ensure_ascii=False)))

    def _ingest_measure(self, batch_id, stats):
        for metric, values in stats.items():
            if not values:
                continue
            row = db.query_one(
                "SELECT * FROM measure_stats WHERE batch_id=? AND metric_name=?",
                (batch_id, metric))
            if row:
                db.execute(
                    "UPDATE measure_stats SET cnt=cnt+?, sum_v=sum_v+?, "
                    "min_v=MIN(min_v,?), max_v=MAX(max_v,?) "
                    "WHERE id=?",
                    (len(values), sum(values), min(values), max(values), row["id"]))
            else:
                db.execute(
                    "INSERT INTO measure_stats(batch_id,metric_name,cnt,sum_v,min_v,max_v)"
                    " VALUES(?,?,?,?,?,?)",
                    (batch_id, metric, len(values), sum(values),
                     min(values), max(values)))
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
