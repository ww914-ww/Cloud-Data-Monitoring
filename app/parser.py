# -*- coding: utf-8 -*-
"""Excel 解析器：把三类报表文件解析成统一的内存结构。

目录约定（相对机台根目录）：
    {项目目录}/{批次目录}/DefectData/Camera*.xlsx     -> 缺陷明细（判定结果 OK/NG）
    {项目目录}/{批次目录}/MeasureData/Camera*.xlsx    -> 测量明细（高度C 等）
    {项目目录}/{批次目录}/MeasureData/*3D测量数据*.xlsx -> 3D 测量（VIN/GND/SW/PIN 最大值/中值）

项目目录名形如 "H5QJGA011_POCO10-4_000 -SW"（机台_项目），
批次目录名形如 "{项目目录}_GD0820B004-L_1C"（项目目录_批次）。
"""
import os

from openpyxl import load_workbook


def split_rel_path(machine_name, rel_path):
    """把相对路径切成 (检测项目, 批次名, 文件类型)。

    rel_path 例: "H5QJGA011_POCO10-4_000 -SW/xxx_GD0820B004-L_1C/DefectData/Camera1.xlsx"
    """
    parts = rel_path.replace("/", os.sep).split(os.sep)
    if len(parts) < 4:
        return None
    project_dir, batch_dir, folder, filename = parts[0], parts[1], parts[2], parts[-1]

    # 项目名 = 项目目录名去掉 "{机台名}_" 前缀
    prefix = machine_name + "_"
    category = project_dir[len(prefix):] if project_dir.startswith(prefix) else project_dir

    # 批次名 = 批次目录名去掉 "{项目目录名}_" 前缀
    prefix2 = project_dir + "_"
    batch_name = batch_dir[len(prefix2):] if batch_dir.startswith(prefix2) else batch_dir

    if folder == "DefectData":
        kind = "defect"
    elif folder == "MeasureData":
        kind = "measure_3d" if "3D测量数据" in filename else "measure"
    else:
        return None

    return category, batch_name, kind


def _to_float(v):
    """报表中数值以字符串存储，安全转 float"""
    if v is None:
        return None
    try:
        return float(str(v).strip())
    except (ValueError, TypeError):
        return None


def parse_defect_file(path, max_defects=50):
    """解析缺陷明细文件。

    判定结果三分：OK=良品 / NG=不良 / 其他值(如"异常")=异常。
    返回 dict:
      ok/ng/other/total: 计数
      rows: [(record_id, time, result)]  全部行
      defects: [(record_id, time, defects)]  仅 NG 行的缺陷明细
    """
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    it = ws.iter_rows(values_only=True)
    header = next(it, None)
    if not header:
        wb.close()
        return {"rows": [], "ok": 0, "ng": 0, "other": 0, "total": 0, "defects": []}

    col = {name: i for i, name in enumerate(header) if name}
    i_id, i_time, i_result = col.get("ID"), col.get("时间"), col.get("判定结果")

    result = {"ok": 0, "ng": 0, "other": 0, "total": 0, "rows": [], "defects": []}
    for row in it:
        rid = row[i_id] if i_id is not None and i_id < len(row) else None
        t = row[i_time] if i_time is not None and i_time < len(row) else None
        verdict = row[i_result] if i_result is not None and i_result < len(row) else None
        if verdict is None or str(verdict).strip() == "":
            continue
        verdict = str(verdict).strip().upper()
        result["total"] += 1
        t = str(t)[:19] if t else None
        row_defects = []
        if verdict == "OK":
            result["ok"] += 1
            result["rows"].append((str(rid), t, verdict))
            continue
        if verdict == "NG":
            result["ng"] += 1
        else:
            result["other"] += 1  # 异常（非 OK/NG 判定）
        for n in range(1, max_defects + 1):
            nm = col.get(f"不良{n}缺陷名")
            if nm is None or nm >= len(row):
                break
            name = row[nm]
            if name is None or str(name).strip() == "":
                continue
            row_defects.append({
                "name": str(name),
                "area": _to_float(row[col[f"不良{n}面积"]]) if f"不良{n}面积" in col else None,
                "width": _to_float(row[col[f"不良{n}宽度"]]) if f"不良{n}宽度" in col else None,
                "height": _to_float(row[col[f"不良{n}高度"]]) if f"不良{n}高度" in col else None,
            })
        result["rows"].append((str(rid), t, verdict))
        result["defects"].append((str(rid), t, row_defects))
    wb.close()
    return result


def parse_measure_file(path):
    """解析测量明细文件（Camera*.xlsx）。

    返回 {metric_name: [float,...]}，跳过 ID/时间等非数值列。
    部分文件没有表头行（第一行直接是数据），此时用 "文件名.列N" 命名指标。
    """
    stem = os.path.splitext(os.path.basename(path))[0]
    return _parse_numeric_sheet(path, col_prefix=stem)


def parse_3d_file(path):
    """解析 3D 测量文件。

    实际结构：'3D上'/'3D下' 两个 sheet 存各项目测量值（VIN/GND/SW/PIN 最大值/中值），
    '最终结果数据' 是 3D 判定结果（序号+OK/NG），同样纳入 OK/NG 统计。
    返回 {metric_name: [float,...]}，指标名带 sheet 前缀区分上下，如 '3D上.VIN最大值'。
    """
    wb = load_workbook(path, read_only=True, data_only=True)
    sheet_names = wb.sheetnames
    wb.close()

    stats = {}
    for name in ("3D上", "3D下"):
        if name in sheet_names:
            sub = _parse_numeric_sheet(path, sheet_name=name)
            for k, v in sub.items():
                stats[f"{name}.{k}"] = v
    return stats


def parse_3d_verdict_file(path):
    """解析 3D '最终结果数据' sheet 的判定结果，返回 (ok, ng, total)。"""
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb["最终结果数据"] if "最终结果数据" in wb.sheetnames else None
    ok = ng = 0
    if ws is not None:
        it = ws.iter_rows(values_only=True)
        header = next(it, None)
        col = {name: i for i, name in enumerate(header) if name}
        i_result = col.get("最终结果")
        if i_result is not None:
            for row in it:
                v = row[i_result] if i_result < len(row) else None
                if v is None:
                    continue
                v = str(v).strip().upper()
                if v == "OK":
                    ok += 1
                elif v == "NG":
                    ng += 1
    wb.close()
    return ok, ng, ok + ng


def _looks_like_data_row(cells):
    """判断一行是否是数据行（而非表头）：第一列纯数字ID、第二列为时间样式"""
    if len(cells) < 2:
        return False
    a, b = cells[0], cells[1]
    if not isinstance(a, str) or not a.isdigit():
        return False
    return isinstance(b, str) and len(b) >= 16 and b[4] == "-" and b[10] == " " and ":" in b


def _parse_numeric_sheet(path, sheet_name=None, col_prefix=None):
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet_name] if sheet_name else wb[wb.sheetnames[0]]
    it = ws.iter_rows(values_only=True)
    header = next(it, None)
    if not header:
        wb.close()
        return {}

    skip = {"ID", "时间", "序号"}

    # 无表头文件：第一行就是数据，指标按 "前缀.列N" 命名
    if _looks_like_data_row([c if c is not None else "" for c in header[:2]]) \
            or (col_prefix and not any(c and str(c).strip() not in skip for c in header[2:])):
        names = {}
        for i in range(len(header)):
            names[i] = f"{col_prefix}.列{i + 1}" if col_prefix else f"列{i + 1}"
        stats = {}
        rows = [header] + list(it)
        for row in rows:
            for i, v in enumerate(row):
                if i < 2:
                    continue  # 跳过 ID 与时间列
                fv = _to_float(v)
                if fv is not None:
                    stats.setdefault(names[i], []).append(fv)
        wb.close()
        return stats

    idxs = [(i, str(h).strip()) for i, h in enumerate(header)
            if h is not None and str(h).strip() not in skip]
    stats = {name: [] for _, name in idxs}

    for row in it:
        for i, name in idxs:
            v = _to_float(row[i]) if i < len(row) else None
            if v is not None:
                stats[name].append(v)
    wb.close()
    return stats
