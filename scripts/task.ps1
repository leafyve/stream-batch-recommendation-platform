param(
    [Parameter(Position = 0, Mandatory = $true)]
    [string]$Command,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Remaining
)

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    & py -3.11 scripts/task.py $Command @Remaining
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}

