@echo off
title Restart GIDI Supervisor
echo =======================================
echo Restarting GIDI Gateway Python Supervisor
echo =======================================
echo Mematikan proses lama di latar belakang...
wmic process where "name='pythonw.exe' and commandline like '%%consumer_%%'" call terminate > nul 2>&1
wmic process where "name='python.exe' and commandline like '%%consumer_%%'" call terminate > nul 2>&1
echo.
echo Menunggu 2 detik...
timeout /T 2 /NOBREAK > nul
echo Menyalakan proses baru...
start "" "%~dp0START.bat"
echo Selesai! Supervisor telah berhasil direstart dan berjalan di latar belakang (tanpa jendela).
pause
