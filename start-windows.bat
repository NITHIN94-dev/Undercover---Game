@echo off
cd /d "%~dp0"
py -m pip install -r requirements.txt
if errorlevel 1 (
  echo.
  echo Could not install the Python dependency. Make sure Python 3.11+ is installed.
  pause
  exit /b 1
)
py server.py
pause
