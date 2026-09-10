# -*- coding: utf-8 -*-
"""规则引擎：自定义标准判定与告警管理。

规则 = 指标 + 判定方向 + 阈值 + 作用范围（机台/检测项目） + 严重级别。
触发时机：批次每有新文件解析入库后调用 evaluate_batch。
告警去重：同一批次同一规则只保留一条 active 告警，数值随批次更新刷新。
"""
from . import database as db

BUILTIN_METRICS = ["良率", "NG数"]

# 一致性校验类型（direction='check' 时 metric 取以下值）
CHECK_METRICS = ["计数平衡", "相机一致性", "缺陷勾稽"]


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


def _run_check(rule, batch):
    """执行一致性校验。返回 (差值, 消息)；差值 <= 容差 视为通过(返回 None)。

    - 计数平衡:  每批次每相机 良品数+不良数+异常数 应等于 生产总数
    - 相机一致性: 同批次各相机(文件)的生产总数应一致
    - 缺陷勾稽:  缺陷明细统计的不良片数 应等于 判定结果的不良数
    """
    tolerance = rule["threshold"] or 0
    check = rule["metric"]

    if check == "计数平衡":
        rows = db.query(
            "SELECT rel_path, ok_count, ng_count, other_count, total_count FROM files "
            "WHERE batch_id=? AND status='done' AND kind='defect' AND total_count>0",
            (batch["id"],))
        bad = []
        max_diff = 0
        for r in rows:
            diff = r["ok_count"] + r["ng_count"] + r["other_count"] - r["total_count"]
            if abs(diff) > tolerance:
                bad.append(f"{_camera_name(r['rel_path'])}: "
                           f"{r['ok_count']}+{r['ng_count']}+{r['other_count']}"
                           f"≠{r['total_count']}(差{diff:+d})")
                max_diff = max(max_diff, abs(diff))
        if bad:
            return max_diff, "计数不平衡 " + "；".join(bad)
        return None

    if check == "相机一致性":
        rows = db.query(
            "SELECT rel_path, total_count FROM files "
            "WHERE batch_id=? AND status='done' AND kind='defect' AND total_count>0",
            (batch["id"],))
        if len(rows) < 2:
            return None  # 只有一个相机，无从比较
        totals = {r["total_count"] for r in rows}
        diff = max(totals) - min(totals)
        if diff > tolerance:
            detail = "；".join(f"{_camera_name(r['rel_path'])}:{r['total_count']}" for r in rows)
            return diff, f"各相机生产总数不一致(最大差{diff}) {detail}"
        return None

    if check == "缺陷勾稽":
        defect_ng = db.query_one(
            "SELECT COUNT(*) AS c FROM ng_details WHERE batch_id=?", (batch["id"],))["c"]
        diff = defect_ng - batch["ng_count"]
        if abs(diff) > tolerance:
            return abs(diff), (f"缺陷明细不良片数 {defect_ng} ≠ 判定结果不良数 "
                               f"{batch['ng_count']}（差{diff:+d}）")
        return None

    return None


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


def _camera_name(rel_path):
    """从相对路径提取相机标识，如 '...\\DefectData\\Camera1.xlsx' -> 'Camera1'"""
    import os
    return os.path.splitext(os.path.basename(rel_path))[0]


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

        # 一致性校验规则
        if rule["direction"] == "check":
            res = _run_check(rule, batch)
            if res is not None:
                diff, msg = res
                db.execute(
                    "INSERT INTO alerts(machine_id,batch_id,rule_id,metric_value,message,"
                    "level,active,created_at,updated_at) VALUES(?,?,?,?,?,?,1,?,?) "
                    "ON CONFLICT(batch_id,rule_id) DO UPDATE SET "
                    "metric_value=excluded.metric_value, message=excluded.message, "
                    "level=excluded.level, active=1, updated_at=excluded.updated_at",
                    (batch["machine_id"], batch_id, rule["id"], diff, msg,
                     rule["level"], db.now_str(), db.now_str()))
            else:
                db.execute("UPDATE alerts SET active=0 WHERE batch_id=? AND rule_id=?",
                           (batch_id, rule["id"]))
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
    """规则编辑界面的指标下拉：内置指标 + 一致性校验类型 + 库中已出现的测量指标"""
    rows = db.query(
        "SELECT DISTINCT metric_name FROM measure_stats ORDER BY metric_name")
    return BUILTIN_METRICS + CHECK_METRICS + [r["metric_name"] for r in rows]
