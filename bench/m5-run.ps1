param(
    [int]$Messages = 20000,
    [int]$Warmup = 2000,
    [int]$Rounds = 3,
    [string]$OutputDir = "result/m5-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$repo = Join-Path $repoRoot ".m2repo"
$buildArgs = @(
    "-pl", "bench", "-am", "package", "-DskipTests", "-Dmaven.test.skip=true",
    "-Dmaven.repo.local=$repo", "-q"
)
& mvn @buildArgs
if ($LASTEXITCODE -ne 0) { throw "M5 benchmark build failed with exit code $LASTEXITCODE" }

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$outputPath = Join-Path $OutputDir ""
$modes = @(
    @{ Name = "produce"; ReadMode = "cold" },
    @{ Name = "consume"; ReadMode = "cold" },
    @{ Name = "consume"; ReadMode = "tail" },
    @{ Name = "mixed"; ReadMode = "cold" }
)
foreach ($mode in $modes) {
    $name = if ($mode.Name -eq "consume") { "consume-$($mode.ReadMode)" } else { $mode.Name }
    $file = Join-Path $outputPath "$name.jsonl"
    $argsValue = "--mode=$($mode.Name) --read-mode=$($mode.ReadMode) --concurrency=1,2,4,8,16,32 --messages=$Messages --warmup=$Warmup --rounds=$Rounds --output=$file"
    & mvn -pl bench exec:java "-Dexec.mainClass=com.minimq.bench.M5Bench" "-Dexec.args=$argsValue" "-Dmaven.repo.local=$repo" -q
    if ($LASTEXITCODE -ne 0) { throw "M5 $name run failed with exit code $LASTEXITCODE" }
}
Write-Host "M5 JSONL results: $((Resolve-Path $OutputDir).Path)"
