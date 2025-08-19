@echo off
echo Installing CNPJ Scraper dependencies...
echo.

echo Installing Python packages...
pip install -r requirements.txt

echo.
echo Installing Playwright browsers...
playwright install chromium

echo.
echo Setup complete! You can now run the scraper with:
echo python main.py
echo.
pause 