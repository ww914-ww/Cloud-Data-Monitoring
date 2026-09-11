# -*- coding: utf-8 -*-
"""一键启动：Web 服务 + 后台采集调度"""
import json
import os
import sys

if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)  # PyInstaller 打包后：exe 所在目录
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, "config.json"), "r", encoding="utf-8") as f:
    CFG = json.load(f)

if __name__ == "__main__":
    os.chdir(BASE_DIR)
    if not getattr(sys, "frozen", False):
        import uvicorn
        # 服务就绪后自动打开浏览器
        import threading
        import webbrowser
        url = f"http://{CFG.get('host', '127.0.0.1')}:{CFG.get('port', 8000)}"
        threading.Timer(2.0, lambda: webbrowser.open(url)).start()
        uvicorn.run("app.main:app", host=CFG.get("host", "127.0.0.1"), port=int(CFG.get("port", 8000)))
    else:
        # 打包模式：uvicorn 以打包内模块方式启动
        import threading
        import webbrowser
        import uvicorn
        from app.main import app as fastapi_app

        url = f"http://{CFG.get('host', '127.0.0.1')}:{CFG.get('port', 8000)}"
        threading.Timer(2.0, lambda: webbrowser.open(url)).start()
        uvicorn.run(fastapi_app, host=CFG.get("host", "127.0.0.1"), port=int(CFG.get("port", 8000)))
