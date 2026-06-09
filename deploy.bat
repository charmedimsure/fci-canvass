@echo off
cd /d "C:\Users\alsal\OneDrive\Desktop\FCI Canvass\fci-deploy"

echo.
echo === Syncing docs root from worker folder ===
copy /Y docs\worker\index.html docs\index.html
copy /Y docs\worker\sw.js docs\sw.js

echo.
set /p MSG="Enter commit message: "

echo.
echo === Committing and pushing to GitHub ===
git add .
git commit -m "%MSG%"
git push

echo.
echo === Deploying Cloudflare Worker ===
cd docs\worker
wrangler deploy

echo.
echo === Done! ===
pause
