# -*- coding: utf-8 -*-
"""一键启动：Web 服务 + 后台采集调度"""
import json
import os
import uvicorn

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, "config.json"), "r", encoding="utf-8") as f:
    CFG = json.load(f)

if __name__ == "__main__":
    os.chdir(BASE_DIR)
    # 服务就绪后自动打开浏览器
    import threading
    import webbrowser
    url = f"http://{CFG.get('host', '127.0.0.1')}:{CFG.get('port', 8000)}"
    threading.Timer(2.0, lambda: webbrowser.open(url)).start()
    uvicorn.run("app.main:app", host=CFG.get("host", "127.0.0.1"), port=int(CFG.get("port", 8000)))
