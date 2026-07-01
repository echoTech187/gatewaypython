@echo off
title GIDI Gateway Python Supervisor
echo =======================================
echo Starting GIDI Gateway Python Supervisor
echo =======================================
echo Please do not close this window.
echo If you close this window, all consumer scripts will stop.
cd /d "%~dp0"
start "" "C:\Users\Pongo\AppData\Local\Programs\Python\Python313\pythonw.exe" consumer_services.py
:: start "" "C:\Users\Pongo\AppData\Local\Programs\Python\Python313\pythonw.exe" consumer_dlq_monitor.py
