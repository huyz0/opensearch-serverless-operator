param(
    [Parameter(Mandatory = $true)][string]$RuntimeLib,
    [Parameter(Mandatory = $true)][string]$WeedExecutable,
    [Parameter(Mandatory = $true)][string]$WorkDirectory
)
$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $WorkDirectory) { throw 'Use a fresh work directory' }
New-Item -ItemType Directory -Force -Path "$WorkDirectory/data", "$WorkDirectory/home/config", "$WorkDirectory/classes" | Out-Null
$gateway = $null
$node = $null
try {
    $gateway = Start-Process $WeedExecutable -WindowStyle Hidden -PassThru `
        -ArgumentList @('server', '-s3', '-s3.port=18333', '-ip=127.0.0.1', "-dir=$WorkDirectory/data", '-volume.max=2') `
        -RedirectStandardOutput "$WorkDirectory/gateway.log" -RedirectStandardError "$WorkDirectory/gateway.err"
    $up = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        Start-Sleep -Seconds 1
        if ($gateway.HasExited) { break }
        try { Invoke-WebRequest -Method Put -Uri 'http://127.0.0.1:18333/operator-test' -TimeoutSec 2 | Out-Null; $up = $true; break } catch {}
    }
    if (-not $up) { throw 'S3 gateway did not start' }
    $env:SMOKE_S3_ACCESS_KEY = 'test-access'
    $env:SMOKE_S3_SECRET_KEY = 'test-secret'
    $classpath = Join-Path $RuntimeLib '*'
    & javac -cp $classpath -d "$WorkDirectory/classes" tools/SmokeKeystore.java
    if ($LASTEXITCODE -ne 0) { throw 'Keystore helper compilation failed' }
    & java -cp "$WorkDirectory/classes;$classpath" SmokeKeystore "$WorkDirectory/home/config"
    if ($LASTEXITCODE -ne 0) { throw 'Keystore creation failed' }
    $arguments = @(
        '--add-opens=java.base/java.lang=ALL-UNNAMED', '--add-opens=java.base/java.nio=ALL-UNNAMED',
        '--add-opens=java.base/java.util=ALL-UNNAMED', '-Dio.netty.noUnsafe=true',
        '-Dio.netty.noKeySetOptimization=true', '-Dio.netty.recycler.maxCapacityPerThread=0',
        '-Dopensearch.set.netty.runtime.available.processors=false', "-Dopensearch.path.conf=$WorkDirectory/home/config",
        '-Dnode.name=s3-smoke', '-Dcluster.name=s3-smoke', "-Dpath.home=$WorkDirectory/home",
        '-Dnetwork.host=127.0.0.1', '-Dhttp.port=19205', '-Dtransport.port=19305', '-Dserverless.roles=ingest',
        '-Dserverless.store.type=s3', '-Dserverless.store.bucket=operator-test', '-Dserverless.store.region=us-east-1',
        '-Dserverless.store.endpoint=http://127.0.0.1:18333', '-Dserverless.store.path_style_access=true',
        '-cp', $classpath, 'org.opensearch.serverless.shell.ServerlessBootstrap'
    )
    $node = Start-Process java -ArgumentList $arguments -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput "$WorkDirectory/node.log" -RedirectStandardError "$WorkDirectory/node.err"
    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        Start-Sleep -Seconds 1
        if ($node.HasExited) { break }
        try { if ((Invoke-RestMethod 'http://127.0.0.1:19205/_serverless/ready' -TimeoutSec 2).ready) { $ready = $true; break } } catch {}
    }
    if (-not $ready) { throw 'Java shell did not become ready' }
    Invoke-RestMethod -Method Put -Uri 'http://127.0.0.1:19205/s3-smoke' -ContentType 'application/json' -Body '{}' | Out-Null
    $written = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        try {
            Invoke-RestMethod -Method Put -Uri 'http://127.0.0.1:19205/s3-smoke/_doc/1?refresh=true' -ContentType 'application/json' -Body '{"message":"s3 test"}' -TimeoutSec 3 | Out-Null
            $written = $true; break
        } catch { Start-Sleep -Seconds 1 }
    }
    if (-not $written) { throw 'Shard did not accept a write within the timeout' }
    $result = Invoke-RestMethod 'http://127.0.0.1:19205/s3-smoke/_doc/1' -TimeoutSec 5
    if (-not $result.found) { throw 'Written document was not found' }
    Stop-Process -Id $node.Id
    $node.WaitForExit()
    New-Item -ItemType Directory -Force -Path "$WorkDirectory/replacement/config" | Out-Null
    & java -cp "$WorkDirectory/classes;$classpath" SmokeKeystore "$WorkDirectory/replacement/config"
    if ($LASTEXITCODE -ne 0) { throw 'Replacement keystore creation failed' }
    $replacementArguments = $arguments | ForEach-Object {
        $_.Replace("$WorkDirectory/home", "$WorkDirectory/replacement").Replace('-Dnode.name=s3-smoke', '-Dnode.name=s3-replacement').Replace('-Dtransport.port=19305', '-Dtransport.port=19306')
    }
    $node = Start-Process java -ArgumentList $replacementArguments -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput "$WorkDirectory/replacement.log" -RedirectStandardError "$WorkDirectory/replacement.err"
    $recovered = $false
    for ($attempt = 0; $attempt -lt 90; $attempt++) {
        try {
            $result = Invoke-RestMethod 'http://127.0.0.1:19205/s3-smoke/_doc/1' -TimeoutSec 3
            if ($result.found -and $result._source.message -eq 's3 test') { $recovered = $true; break }
        } catch {}
        if ($node.HasExited) { break }
        Start-Sleep -Seconds 1
    }
    if (-not $recovered) { throw 'Replacement did not recover the acknowledged document' }
    Write-Output 'S3-backed shell indexed and recovered a document after process replacement.'
} finally {
    if ($node -and -not $node.HasExited) { Stop-Process -Id $node.Id }
    if ($gateway -and -not $gateway.HasExited) { Stop-Process -Id $gateway.Id }
    Remove-Item Env:SMOKE_S3_ACCESS_KEY, Env:SMOKE_S3_SECRET_KEY -ErrorAction SilentlyContinue
}
