$ErrorActionPreference = "Continue"

$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$exe = Join-Path $repo "bin\Debug\net10.0\AutoMateAPI.exe"
$localSettings = Join-Path $repo "appsettings.Local.json"
$log = Join-Path $env:USERPROFILE "Desktop\withemail.log"

Set-Location $repo

"==== withemail start $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ====" | Tee-Object -FilePath $log -Append

if (-not (Test-Path -LiteralPath $exe)) {
    "Build output not found: $exe" | Tee-Object -FilePath $log -Append
    "Run dotnet build from $repo, then try again." | Tee-Object -FilePath $log -Append
    Read-Host "Press Enter to close"
    exit 1
}

$hasEnvDatabaseUrl = -not [string]::IsNullOrWhiteSpace($env:DATABASE_PUBLIC_URL)
$hasLocalDatabaseUrl = $false

if (Test-Path -LiteralPath $localSettings) {
    try {
        $localJson = Get-Content -LiteralPath $localSettings -Raw | ConvertFrom-Json
        $hasLocalDatabaseUrl = -not [string]::IsNullOrWhiteSpace($localJson.DATABASE_PUBLIC_URL)
    }
    catch {
        "Could not read appsettings.Local.json: $($_.Exception.Message)" | Tee-Object -FilePath $log -Append
    }
}

if (-not $hasEnvDatabaseUrl -and -not $hasLocalDatabaseUrl) {
    "DATABASE_PUBLIC_URL is missing. The API cannot start without the Railway/Postgres connection string." | Tee-Object -FilePath $log -Append
    "Set DATABASE_PUBLIC_URL in Windows or appsettings.Local.json, then run withemail again." | Tee-Object -FilePath $log -Append
    Read-Host "Press Enter to close"
    exit 1
}

$existingProcess = Get-Process -Name "AutoMateAPI" -ErrorAction SilentlyContinue |
    Where-Object { $_.Path -eq $exe } |
    Select-Object -First 1

if ($existingProcess) {
    "AutoMate API is already running from this build." | Tee-Object -FilePath $log -Append
    "Process ID: $($existingProcess.Id)" | Tee-Object -FilePath $log -Append
    "Local URL: http://127.0.0.1:5000" | Tee-Object -FilePath $log -Append
    Read-Host "Press Enter to close"
    exit 0
}

& $exe 2>&1 | Tee-Object -FilePath $log -Append
$exitCode = $LASTEXITCODE

"==== withemail exit $exitCode $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ====" | Tee-Object -FilePath $log -Append
Read-Host "Press Enter to close"
exit $exitCode
