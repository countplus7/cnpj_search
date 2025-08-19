@echo off
echo Killing all Python processes...
taskkill /f /im python.exe
echo.
echo Checking if any Python processes remain...
tasklist | findstr python
if %errorlevel% equ 0 (
    echo Some Python processes are still running. Force killing...
    taskkill /f /im python.exe /t
) else (
    echo All Python processes terminated successfully.
)
echo.
echo Done. Press any key to exit.
pause >nul 