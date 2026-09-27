"""
AlgoTrader 100% Python F&O Bot & Cyberpunk Desk
Main application entry point for local execution and cloud deployment (Render).
"""
import os
import sys
from pathlib import Path

# Add backend directory to Python sys.path
BACKEND_DIR = Path(__file__).resolve().parent / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

# Ensure UTF-8 output on Windows consoles
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from server import app, start

if __name__ == "__main__":
    start()
