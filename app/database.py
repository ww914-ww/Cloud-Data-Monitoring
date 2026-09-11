# -*- coding: utf-8 -*-
"""SQLite 数据库层：建表、连接管理与基础数据访问。

多线程说明：采集线程（写）与 Web 请求线程（读写）共用一个连接，
用全局锁串行化所有操作，SQLite 本身开启 WAL 提高并发容忍度。
"""
import json
import os
import sqlite3
import sys
import threading
from datetime import datetime


def _base_dir():
    """项目根目录：源码运行=项目文件夹；PyInstaller 打包=exe 所在目录"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


BASE_DIR = _base_dir()
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "monitor.db")
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

_conn = None
_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS machines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    online INTEGER NOT NULL DEFAULT 0,
    last_scan_at TEXT,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    metric TEXT NOT NULL,              -- 良率 / NG数 / 测量指标名(如 PIN21最大值、高度C)
    direction TEXT NOT NULL,           -- below 低于 / above 高于 / range 超出区间
    threshold REAL,                    -- below/above 的阈值
    threshold_high REAL,               -- range 的上限(threshold 为下限)
    machine_id INTEGER,               -- NULL = 全部机台
    category TEXT,                     -- NULL = 全部检测项目
    level TEXT NOT NULL DEFAULT 'warning',  -- info / warning / critical
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_id INTEGER NOT NULL,
    rel_path TEXT NOT NULL,
    kind TEXT,                         -- defect / measure / measure_3d
    size INTEGER,
    mtime REAL,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending/stable/done/error
    first_size INTEGER,               -- 首次发现时的大小(用于传输完成检测)
    first_seen TEXT,
    parsed_at TEXT,
    error TEXT,
    ok_count INTEGER DEFAULT 0,       -- 相机级(文件级)良品数
    ng_count INTEGER DEFAULT 0,       -- 相机级不良数
    other_count INTEGER DEFAULT 0,    -- 相机级异常数(非OK/NG判定)
    total_count INTEGER DEFAULT 0,    -- 相机级生产总数
    batch_id INTEGER,                 -- 所属批次
    UNIQUE(machine_id, rel_path)
);

CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_id INTEGER NOT NULL,
    category TEXT NOT NULL,            -- 检测项目
    batch_name TEXT NOT NULL,
    total INTEGER NOT NULL DEFAULT 0,
    ok_count INTEGER NOT NULL DEFAULT 0,
    ng_count INTEGER NOT NULL DEFAULT 0,
    other_count INTEGER NOT NULL DEFAULT 0,   -- 异常数(非OK/NG判定)
    first_time TEXT,
    last_time TEXT,
    updated_at TEXT,
    UNIQUE(machine_id, category, batch_name)
);

CREATE TABLE IF NOT EXISTS ng_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL,
    record_id TEXT,
    time TEXT,
    defects TEXT                        -- 缺陷摘要 JSON [{name,area,width,height}]
);
CREATE INDEX IF NOT EXISTS idx_ng_batch ON ng_details(batch_id);

CREATE TABLE IF NOT EXISTS measure_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    cnt INTEGER NOT NULL DEFAULT 0,
    sum_v REAL NOT NULL DEFAULT 0,
    min_v REAL,
    max_v REAL,
    UNIQUE(batch_id, metric_name)
);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_id INTEGER NOT NULL,
    batch_id INTEGER NOT NULL,
    rule_id INTEGER NOT NULL,
    metric_value REAL,
    message TEXT,
    level TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(batch_id, rule_id)
);
"""


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_conn():
    global _conn
    if _conn is None:
        os.makedirs(DATA_DIR, exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")  # WAL 下安全且大幅减少 fsync
        _conn.execute("PRAGMA busy_timeout=10000")
        _conn.executescript(SCHEMA)
        _migrate(_conn)
        _conn.commit()
    return _conn


# 已有库的结构升级：缺失列自动补齐（存量行按默认 0 处理）
_MIGRATIONS = [
    ("batches", "other_count", "ALTER TABLE batches ADD COLUMN other_count INTEGER NOT NULL DEFAULT 0"),
    ("files", "ok_count", "ALTER TABLE files ADD COLUMN ok_count INTEGER DEFAULT 0"),
    ("files", "ng_count", "ALTER TABLE files ADD COLUMN ng_count INTEGER DEFAULT 0"),
    ("files", "other_count", "ALTER TABLE files ADD COLUMN other_count INTEGER DEFAULT 0"),
    ("files", "total_count", "ALTER TABLE files ADD COLUMN total_count INTEGER DEFAULT 0"),
    ("files", "batch_id", "ALTER TABLE files ADD COLUMN batch_id INTEGER"),
]


def _migrate(conn):
    for _table, column, ddl in _MIGRATIONS:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({_table})")]
        if column not in cols:
            conn.execute(ddl)


def execute(sql, params=()):
    """线程安全执行写操作，返回 lastrowid"""
    with _lock:
        conn = get_conn()
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.lastrowid


def execute_many(sql, seq):
    """线程安全批量执行写操作（单事务，避免逐行提交）"""
    with _lock:
        conn = get_conn()
        conn.executemany(sql, seq)
        conn.commit()


def query(sql, params=()):
    """线程安全查询，返回 list[dict]"""
    with _lock:
        conn = get_conn()
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def query_one(sql, params=()):
    rows = query(sql, params)
    return rows[0] if rows else None


def init_default_machines():
    """从 config.json 读取默认机台（仅首次插入，不覆盖已改动的路径）"""
    if not os.path.exists(CONFIG_PATH):
        return
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    for m in cfg.get("default_machines", []):
        path = m["path"]
        if not os.path.isabs(path):
            path = os.path.join(BASE_DIR, path)
        exists = query_one("SELECT id FROM machines WHERE name=?", (m["name"],))
        if not exists:
            execute(
                "INSERT INTO machines(name, path, enabled) VALUES(?,?,1)",
                (m["name"], path),
            )


DEFAULT_RULES = [
    # 预置规则示例，用户可在界面上随意修改/停用/删除
    dict(name="良率下限", metric="良率", direction="below", threshold=95,
         threshold_high=None, level="critical"),
    dict(name="良率上限(疑漏检)", metric="良率", direction="above", threshold=99.8,
         threshold_high=None, level="warning"),
    dict(name="单批次NG数", metric="NG数", direction="above", threshold=50,
         threshold_high=None, level="warning"),
    # 一致性校验：threshold 为容差(0 表示必须完全相等)
    dict(name="计数平衡校验", metric="计数平衡", direction="check", threshold=0,
         threshold_high=None, level="critical"),
    dict(name="相机一致性校验", metric="相机一致性", direction="check", threshold=0,
         threshold_high=None, level="critical"),
    dict(name="缺陷勾稽校验", metric="缺陷勾稽", direction="check", threshold=0,
         threshold_high=None, level="critical"),
]


def init_default_rules():
    cnt = query_one("SELECT COUNT(*) AS c FROM rules")["c"]
    if cnt == 0:
        for r in DEFAULT_RULES:
            execute(
                "INSERT INTO rules(name,metric,direction,threshold,threshold_high,"
                "machine_id,category,level,enabled,created_at)"
                " VALUES(?,?,?,?,?,NULL,NULL,?,1,?)",
                (r["name"], r["metric"], r["direction"], r["threshold"],
                 r["threshold_high"], r["level"], now_str()),
            )
