# update-bridge.ps1 — sync this clone to origin/main the reliable way.
#
# Plain `git pull` keeps stalling on this machine: the CI rewrites the auto-published
# data files (signals.json / prices.json / equity-executions.json ...) every few minutes,
# so a merge hits a conflict on them and silently aborts, leaving the clone behind. A
# hard reset to origin/main sidesteps that — the data files regenerate, and the bridge
# pushes via the GitHub API (not this clone), so nothing you need is lost.
#
#   .\update-bridge.ps1          # fetch + hard-reset + verify (then restart the bridge yourself)
#   .\update-bridge.ps1 -Run     # ... and relaunch the bridge afterwards (poll 900)
#
# Stop the running bridge (Ctrl+C in its window) before running this.

param([switch]$Run)

$ErrorActionPreference = "Stop"
Write-Host "Fetching origin/main..." -ForegroundColor Cyan
git fetch origin main
Write-Host "Hard-resetting to origin/main (discards local data-file churn)..." -ForegroundColor Cyan
git reset --hard origin/main
Write-Host ""
Write-Host "Now at:" -ForegroundColor Green
git log --oneline -1

# Verify the loaded universe so you can see the sync actually took.
Write-Host ""
Write-Host "Symbol universe:" -ForegroundColor Green
python -c "import equity_bridge as e; from collections import Counter; m={'NAS':'US','NYSE':'US','ETR':'DE','LSE':'UK','PAR':'FR','TSE':'JP','HK':'HK','MAD':'ES'}; c=Counter(m.get(v.split('.')[-1],v.split('.')[-1]) for v in e.SYMBOLS.values()); print('  total',len(e.SYMBOLS),dict(c))"

if ($Run) {
    Write-Host ""
    Write-Host "Launching bridge (--poll 900)..." -ForegroundColor Cyan
    python equity_bridge.py --poll 900
} else {
    Write-Host ""
    Write-Host "Done. Restart the bridge:  python equity_bridge.py --poll 900" -ForegroundColor Yellow
}
