# -*- coding: utf-8 -*-
"""规则引擎：自定义标准判定与告警管理。

规则 = 指标 + 判定方向 + 阈值 + 作用范围（机台/检测项目） + 严重级别。
触发时机：批次每有新文件解析入库后调用 evaluate_batch。
告警去重：同一批次同一规则只保留一条 active 告警，数值随批次更新刷新。
"""
from . import database as db

BUILTIN_METRICS = ["良率", "NG数"]


def get_metric_value(rule, batch):
    """返回规则指标的 (代表值, min, max)；None 表示该批次无此数据。

    内置指标（良率/NG数）为标量，min=max=值；
    测量指标返回该批次的最小值/最大值（below 看 min，above 看 max）。
    """
    m = rule["metric"]
    if m == "良率":
        if not batch["total"]:
            return None
        v = batch["ok_count"] / batch["total"] * 100
        return (v, v, v)
    if m == "NG数":
        v = batch["ng_count"]
        return (v, v, v)
    ms = db.query_one(
        "SELECT * FROM measure_stats WHERE batch_id=? AND metric_name=?",
        (batch["id"], m))
    if not ms or not ms["cnt"]:
        return None
    return (ms["min_v"], ms["min_v"], ms["max_v"])


def _hit(rule, mn, mx):
    d = rule["direction"]
    t = rule["threshold"]
    high = rule["threshold_high"]
    if d == "below":
        return t is not None and mn < t
    if d == "above":
        return t is not None and mx > t
    # range: 超出区间 [t, high]
    return (t is not None and mn < t) or (high is not None and mx > high)


def _fmt(rule, mn, mx):
    """生成告警消息文本"""
    m = rule["metric"]
    d = rule["direction"]
    t = rule["threshold"]
    high = rule["threshold_high"]
    if m == "良率":
        if d == "below":
            return f"良率 {mn:.2f}% 低于下限 {t}%"
        if d == "above":
            return f"良率 {mx:.2f}% 高于上限 {t}%（疑漏检）"
        return f"良率 {mn:.2f}% 超出区间 [{t}, {high}]%"
    if m == "NG数":
        if d == "above":
            return f"NG数 {mx} 超过上限 {t:g}"
        if d == "below":
            return f"NG数 {mn} 低于下限 {t:g}"
        return f"NG数 {mx} 超出区间 [{t:g}, {high:g}]"
    # 测量指标
    if d == "below":
        return f"{m} 最小值 {mn:.4g} 低于阈值 {t:g}"
    if d == "above":
        return f"{m} 最大值 {mx:.4g} 高于阈值 {t:g}"
    return f"{m} 超出区间 [{t:g}, {high:g}]（min={mn:.4g}, max={mx:.4g}）"


def evaluate_batch(batch_id):
    """对指定批次跑全部启用的匹配规则，upsert/解除告警。"""
    batch = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    if not batch:
        return

    rules_all = db.query("SELECT * FROM rules WHERE enabled=1")
    for rule in rules_all:
        # 作用范围过滤
        if rule["machine_id"] and rule["machine_id"] != batch["machine_id"]:
            continue
        if rule["category"] and rule["category"] != batch["category"]:
            continue

        vals = get_metric_value(rule, batch)
        if vals is None:
            db.execute("UPDATE alerts SET active=0 WHERE batch_id=? AND rule_id=?",
                       (batch_id, rule["id"]))
            continue

        value, mn, mx = vals
        if _hit(rule, mn, mx):
            msg = _fmt(rule, mn, mx)
            db.execute(
                "INSERT INTO alerts(machine_id,batch_id,rule_id,metric_value,message,"
                "level,active,created_at,updated_at) VALUES(?,?,?,?,?,?,1,?,?) "
                "ON CONFLICT(batch_id,rule_id) DO UPDATE SET "
                "metric_value=excluded.metric_value, message=excluded.message, "
                "level=excluded.level, active=1, updated_at=excluded.updated_at",
                (batch["machine_id"], batch_id, rule["id"], value, msg,
                 rule["level"], db.now_str(), db.now_str()))
        else:
            db.execute("UPDATE alerts SET active=0 WHERE batch_id=? AND rule_id=?",
                       (batch_id, rule["id"]))


def available_metrics():
    """规则编辑界面的指标下拉：内置指标 + 库中已出现的测量指标"""
    rows = db.query(
        "SELECT DISTINCT metric_name FROM measure_stats ORDER BY metric_name")
    return BUILTIN_METRICS + [r["metric_name"] for r in rows]
