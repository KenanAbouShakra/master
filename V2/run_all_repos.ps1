$repos = @(
    "docker/cli",
    "prometheus/prometheus",
    "tektoncd/pipeline",
    "helm/helm",
    "pytest-dev/pytest",
    "pandas-dev/pandas",
    "open-telemetry/opentelemetry-collector",
    "grafana/grafana",
    "apache/airflow",
    "home-assistant/core",
    "cli/cli"
)

$progressFile = "COLLECTION_PROGRESS.md"

# Write header if file does not exist yet
if (-not (Test-Path $progressFile)) {
    @"
# Collection Progress

| Repository | Workflows | PRs | Reviews | Commits | Runs | Attempts | Jobs | PR-CI Links | Status |
|---|---|---|---|---|---|---|---|---|---|
"@ | Set-Content $progressFile -Encoding UTF8
}

foreach ($repo in $repos) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "Starting: $repo" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan

    $config = Get-Content config.yaml -Raw
    $config = $config -replace '(?m)^repositories:[\s\S]*?(?=\n\w|\z)', "repositories:`n  - `"$repo`""
    $config | Set-Content config.yaml -Encoding UTF8

    python collect_all.py --config config.yaml

    if ($LASTEXITCODE -ne 0) {
        Write-Host "FEIL i $repo (exit $LASTEXITCODE) - fortsetter med neste..." -ForegroundColor Red
        # Append failed row
        $statsLine = python repo_stats.py $repo 2>$null
        if ($statsLine) {
            ($statsLine -replace '\|$', '| FEIL |') | Add-Content $progressFile -Encoding UTF8
        } else {
            "| $repo | - | - | - | - | - | - | - | - | FEIL |" | Add-Content $progressFile -Encoding UTF8
        }
    } else {
        Write-Host "Ferdig: $repo" -ForegroundColor Green
        $statsLine = python repo_stats.py $repo 2>$null
        if ($statsLine) {
            ($statsLine -replace '\|$', '| OK |') | Add-Content $progressFile -Encoding UTF8
        } else {
            "| $repo | - | - | - | - | - | - | - | - | OK |" | Add-Content $progressFile -Encoding UTF8
        }
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "Alle repoer ferdig! Se COLLECTION_PROGRESS.md" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
