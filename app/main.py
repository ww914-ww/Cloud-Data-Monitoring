# -*- coding: utf-8 -*-
"""FastAPI 入口：Web 界面 + API + 启动采集调度。

启动：python run.py  ->  浏览器访问 http://127.0.0.1:8000
"""
import json
import os
import sys
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from . import database as db
from . import rules
from . import scanner


def _base_dir():
    """项目根目录：源码运行=项目文件夹；PyInstaller 打包=exe 所在目录"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _static_dir():
    """前端页面目录：源码运行=app/static；打包后=解包资源内 app/static"""
    if getattr(sys, "frozen", False):
        return os.path.join(sys._MEIPASS, "app", "static")
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


VALID_DIRECTIONS = ("below", "above", "range", "check")
CHECK_METRICS = ("计数平衡", "相机一致性", "缺陷勾稽")


def _validate_rule(body):
    """规则参数公共校验"""
    if body.direction not in VALID_DIRECTIONS:
        raise HTTPException(400, "direction 必须是 below/above/range/check")
    if body.level not in ("info", "warning", "critical"):
        raise HTTPException(400, "level 必须是 info/warning/critical")
    if body.direction == "check":
        if body.metric not in CHECK_METRICS:
            raise HTTPException(400, "一致性校验的指标必须是：" + "/".join(CHECK_METRICS))
    else:
        if body.metric in CHECK_METRICS:
            raise HTTPException(400, f"指标 {body.metric} 需要选择「一致性校验」判定方式")
        if body.direction == "range" and (body.threshold is None or body.threshold_high is None):
            raise HTTPException(400, "区间判定需要同时提供下限和上限")
        if body.direction != "range" and body.threshold is None:
            raise HTTPException(400, "请提供阈值")

BASE_DIR = _base_dir()
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CFG = json.load(f)
SCAN_INTERVAL = CFG.get("scan_interval", 60)


@asynccontextmanager
async def lifespan(_app):
    db.init_default_machines()
    db.init_default_rules()
    scanner.start_scanners(SCAN_INTERVAL)
    yield


app = FastAPI(title="云盘报表数据监控", lifespan=lifespan)

STATIC_DIR = _static_dir()


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# ---------------- 机台 ----------------

class MachineIn(BaseModel):
    name: str
    path: str


class MachineEdit(BaseModel):
    name: str | None = None
    path: str | None = None
    enabled: bool | None = None


@app.get("/api/machines")
def list_machines():
    rows = db.query("""
        SELECT m.*,
            (SELECT COUNT(*) FROM alerts a WHERE a.machine_id=m.id AND a.active=1) AS alert_cnt
        FROM machines m ORDER BY m.id""")
    for r in rows:
        r["online"] = bool(r["online"])
        r["enabled"] = bool(r["enabled"])
    return rows


@app.post("/api/machines")
def add_machine(body: MachineIn):
    if not body.name.strip() or not body.path.strip():
        raise HTTPException(400, "机台名称与路径不能为空")
    if db.query_one("SELECT id FROM machines WHERE name=?", (body.name,)):
        raise HTTPException(400, f"机台 {body.name} 已存在")
    path = body.path.strip()
    if not os.path.isabs(path):
        path = os.path.join(BASE_DIR, path)
    # 路径暂不可达也允许登记：网络恢复后扫描线程自动上线
    warn = None if os.path.exists(path) else "路径当前不可达，已登记，网络恢复后自动开始采集"
    mid = db.execute("INSERT INTO machines(name,path,enabled) VALUES(?,?,1)",
                     (body.name.strip(), path))
    scanner.start_machine(mid, SCAN_INTERVAL)
    return {"id": mid, "warning": warn}


@app.put("/api/machines/{mid}")
def edit_machine(mid: int, body: MachineEdit):
    """修改机台（名称/路径/启停），路径或启停变化后扫描线程立即重启。

    路径不校验存在性：允许先登记，网络恢复后自动上线扫描。
    """
    m = db.query_one("SELECT * FROM machines WHERE id=?", (mid,))
    if not m:
        raise HTTPException(404, "机台不存在")
    name = body.name.strip() if body.name else m["name"]
    path = body.path.strip() if body.path else m["path"]
    enabled = m["enabled"]
    if body.enabled is not None:
        enabled = 1 if body.enabled else 0
    if not name or not path:
        raise HTTPException(400, "机台名称与路径不能为空")
    dup = db.query_one("SELECT id FROM machines WHERE name=? AND id<>?", (name, mid))
    if dup:
        raise HTTPException(400, f"机台 {name} 已存在")

    changed = (path != m["path"]) or (enabled != m["enabled"]) or (name != m["name"])
    db.execute("UPDATE machines SET name=?, path=?, enabled=? WHERE id=?",
               (name, path, enabled, mid))
    if changed:
        scanner.restart_machine(mid, SCAN_INTERVAL)
    return {"ok": True}


# ---------------- 批次与数据 ----------------

@app.get("/api/batches")
def list_batches(machine_id: int | None = None):
    sql = """
        SELECT b.*, m.name AS machine_name,
            (SELECT MAX(CASE a.level WHEN 'critical' THEN 2 WHEN 'warning' THEN 1 ELSE 0 END)
             FROM alerts a WHERE a.batch_id=b.id AND a.active=1) AS alert_rank
        FROM batches b JOIN machines m ON m.id=b.machine_id"""
    params = ()
    if machine_id:
        sql += " WHERE b.machine_id=?"
        params = (machine_id,)
    sql += " ORDER BY b.updated_at DESC"
    rows = db.query(sql, params)
    for r in rows:
        r["yield_pct"] = round(r["ok_count"] / r["total"] * 100, 2) if r["total"] else None
    return rows


@app.get("/api/batches/{batch_id}")
def batch_detail(batch_id: int):
    batch = db.query_one("""
        SELECT b.*, m.name AS machine_name FROM batches b
        JOIN machines m ON m.id=b.machine_id WHERE b.id=?""", (batch_id,))
    if not batch:
        raise HTTPException(404, "批次不存在")
    batch["yield_pct"] = round(batch["ok_count"] / batch["total"] * 100, 2) if batch["total"] else None
    batch["ng_details"] = db.query(
        "SELECT * FROM ng_details WHERE batch_id=? ORDER BY time LIMIT 500", (batch_id,))
    for n in batch["ng_details"]:
        n["defects"] = json.loads(n["defects"]) if n["defects"] else []
    batch["measure_stats"] = db.query(
        "SELECT metric_name, cnt, min_v, max_v, "
        "ROUND(sum_v/cnt, 6) AS avg_v FROM measure_stats "
        "WHERE batch_id=? ORDER BY metric_name", (batch_id,))
    batch["alerts"] = db.query(
        "SELECT * FROM alerts WHERE batch_id=? AND active=1 ORDER BY level", (batch_id,))
    return batch


# ---------------- 告警 ----------------

@app.get("/api/alerts")
def list_alerts(machine_id: int | None = None, active_only: bool = False):
    sql = """SELECT a.*, m.name AS machine_name, b.category, b.batch_name, r.name AS rule_name
             FROM alerts a JOIN machines m ON m.id=a.machine_id
             JOIN batches b ON b.id=a.batch_id JOIN rules r ON r.id=a.rule_id"""
    conds, params = [], []
    if machine_id:
        conds.append("a.machine_id=?")
        params.append(machine_id)
    if active_only:
        conds.append("a.active=1")
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY a.active DESC, a.updated_at DESC LIMIT 500"
    return db.query(sql, params)


# ---------------- 规则变化后的重判 ----------------
# 原则：不做全量重判。
#   删除/停用规则 -> 仅反激活该规则的告警（一条 UPDATE）
#   新增/修改/启用规则 -> SQL 筛出"受影响批次"（范围 + 指标有数据），
#                         后台线程重判，API 立即返回，前端显示进度。
_rejudge_epoch = 0            # 每次触发 +1，旧线程发现变化自动让位
_rejudge_state = {"running": False, "done": 0, "total": 0}


def _affected_batch_ids(rule):
    """筛出该规则可能产生判定变化的批次 id 列表（SQL 集合筛选，不做逐批查询）"""
    sql = "SELECT b.id FROM batches b WHERE 1=1"
    params = []
    if rule["machine_id"]:
        sql += " AND b.machine_id=?"
        params.append(rule["machine_id"])
    if rule["category"]:
        sql += " AND b.category=?"
        params.append(rule["category"])
    m = rule["metric"]
    if m in ("良率", "NG数"):
        sql += " AND b.total>0"           # 有判定数据的批次
    elif m in CHECK_METRICS:
        sql += " AND EXISTS (SELECT 1 FROM files f WHERE f.batch_id=b.id" \
               " AND f.kind='defect' AND f.status='done')"  # 一致性校验：有缺陷文件的批次
    else:
        sql += " AND EXISTS (SELECT 1 FROM measure_stats ms WHERE ms.batch_id=b.id" \
               " AND ms.metric_name=?)"   # 测量类：含该指标的批次
        params.append(m)
    return [r["id"] for r in db.query(sql, params)]


def _rejudge_async(rule):
    """后台重判受影响批次。连续多次改动时旧线程自动让位（epoch 机制）。"""
    global _rejudge_epoch
    _rejudge_epoch += 1
    epoch = _rejudge_epoch
    ids = _affected_batch_ids(rule)
    threading.Thread(target=_rejudge_worker, args=(ids, epoch), daemon=True).start()


def _rejudge_worker(ids, epoch):
    _rejudge_state.update(running=True, done=0, total=len(ids))
    try:
        for i, bid in enumerate(ids):
            if epoch != _rejudge_epoch:
                return  # 有更新的规则改动接管了重判
            rules.evaluate_batch(bid)
            _rejudge_state["done"] = i + 1
    finally:
        if epoch == _rejudge_epoch:
            _rejudge_state.update(running=False)


# ---------------- 规则 ----------------

class RuleIn(BaseModel):
    name: str
    metric: str
    direction: str  # below / above / range
    threshold: float | None = None
    threshold_high: float | None = None
    machine_id: int | None = None
    category: str | None = None
    level: str = "warning"  # info / warning / critical
    enabled: bool = True


@app.get("/api/rules")
def list_rules():
    rows = db.query("""SELECT r.*, m.name AS machine_name FROM rules r
                       LEFT JOIN machines m ON m.id=r.machine_id ORDER BY r.id""")
    for r in rows:
        r["enabled"] = bool(r["enabled"])
    return rows


@app.get("/api/metrics")
def list_metrics():
    return rules.available_metrics()


@app.post("/api/rules")
def add_rule(body: RuleIn):
    _validate_rule(body)
    rid = db.execute(
        "INSERT INTO rules(name,metric,direction,threshold,threshold_high,machine_id,"
        "category,level,enabled,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (body.name.strip(), body.metric, body.direction, body.threshold,
         body.threshold_high, body.machine_id, body.category or None,
         body.level, 1 if body.enabled else 0, db.now_str()))
    if body.enabled:
        _rejudge_async(body.model_dump())
    return {"id": rid}


@app.put("/api/rules/{rid}")
def update_rule(rid: int, body: RuleIn):
    row = db.query_one("SELECT * FROM rules WHERE id=?", (rid,))
    if not row:
        raise HTTPException(404, "规则不存在")
    _validate_rule(body)
    db.execute(
        "UPDATE rules SET name=?,metric=?,direction=?,threshold=?,threshold_high=?,"
        "machine_id=?,category=?,level=?,enabled=? WHERE id=?",
        (body.name.strip(), body.metric, body.direction, body.threshold,
         body.threshold_high, body.machine_id, body.category or None,
         body.level, 1 if body.enabled else 0, rid))
    if body.enabled:
        _rejudge_async(body.model_dump())
    else:
        # 停用：只需解除该规则的活动告警，无需重判
        db.execute("UPDATE alerts SET active=0 WHERE rule_id=?", (rid,))
    return {"ok": True}


@app.delete("/api/rules/{rid}")
def delete_rule(rid: int):
    # 删除规则：其告警一并删除，其他规则的判定不受影响，无需重判
    db.execute("DELETE FROM alerts WHERE rule_id=?", (rid,))
    db.execute("DELETE FROM rules WHERE id=?", (rid,))
    return {"ok": True}


@app.get("/api/rejudge-status")
def rejudge_status():
    return _rejudge_state
