@echo off
title Stop GIDI Supervisor
echo =======================================
echo Stopping GIDI Gateway Python Supervisor
echo =======================================
echo Menghentikan seluruh proses consumer di latar belakang...
wmic process where "name='pythonw.exe' and commandline like '%%consumer_%%'" call terminate > nul 2>&1
wmic process where "name='python.exe' and commandline like '%%consumer_%%'" call terminate > nul 2>&1
echo.
echo Selesai! Semua consumer berhasil dihentikan.
pause
