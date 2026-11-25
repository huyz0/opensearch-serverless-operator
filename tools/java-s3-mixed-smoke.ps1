param(
    [Parameter(Mandatory = $true)][string]$RuntimeLib,
    [Parameter(Mandatory = $true)][string]$WeedExecutable,
    [Parameter(Mandatory = $true)][string]$WorkDirectory,
    [int]$S3Port = 18333,
    [int]$IngestPort = 19471,
    [int]$SearchPort = 19472
)
$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $WorkDirectory) { throw 'Use a fresh work directory' }
New-Item -ItemType Directory -Force -Path "$WorkDirectory/data", "$WorkDirectory/keys", "$WorkDirectory/classes" | Out-Null
$gateway = $null
try {
    $gateway = Start-Process $WeedExecutable -WindowStyle Hidden -PassThru -ArgumentList @('server', '-s3', "-s3.port=$S3Port", '-ip=127.0.0.1', "-dir=$WorkDirectory/data", '-volume.max=2') -RedirectStandardOutput "$WorkDirectory/gateway.log" -RedirectStandardError "$WorkDirectory/gateway.err"
    $endpoint = "http://127.0.0.1:$S3Port"
    $up = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        if ($gateway.HasExited) { break }
        try { Invoke-WebRequest -Method Put -Uri "$endpoint/operator-test" -TimeoutSec 2 | Out-Null; $up = $true; break } catch { Start-Sleep -Seconds 1 }
    }
    if (-not $up) { throw 'Local S3 gateway did not start' }
    $env:SMOKE_S3_ACCESS_KEY = 'test-access'
    $env:SMOKE_S3_SECRET_KEY = 'test-secret'
    $classpath = Join-Path $RuntimeLib '*'
    & javac -cp $classpath -d "$WorkDirectory/classes" tools/SmokeKeystore.java
    if ($LASTEXITCODE -ne 0) { throw 'Keystore helper compilation failed' }
    & java -cp "$WorkDirectory/classes;$classpath" SmokeKeystore "$WorkDirectory/keys"
    if ($LASTEXITCODE -ne 0) { throw 'Keystore creation failed' }
    $env:OPERATOR_TEST_MIXED_FLEET = '1'
    $env:OPERATOR_TEST_CANDIDATE_OWNS = '1'
    & "$PSScriptRoot/java-role-smoke.ps1" -RuntimeLib $RuntimeLib -WorkDirectory "$WorkDirectory/nodes" -IngestPort $IngestPort -SearchPort $SearchPort -S3Endpoint $endpoint -S3Bucket 'operator-test' -S3Keystore "$WorkDirectory/keys/opensearch.keystore"
} finally {
    if ($gateway -and -not $gateway.HasExited) { Stop-Process -Id $gateway.Id }
    Remove-Item Env:SMOKE_S3_ACCESS_KEY, Env:SMOKE_S3_SECRET_KEY, Env:OPERATOR_TEST_MIXED_FLEET, Env:OPERATOR_TEST_CANDIDATE_OWNS -ErrorAction SilentlyContinue
}
