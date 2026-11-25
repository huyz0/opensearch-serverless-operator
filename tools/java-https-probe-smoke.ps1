param(
    [Parameter(Mandatory = $true)][string]$RuntimeLib,
    [Parameter(Mandatory = $true)][string]$WorkDirectory,
    [Parameter(Mandatory = $true)][string]$TlsDirectory,
    [Parameter(Mandatory = $true)][string]$HealthProbe,
    [int]$HttpPort = 19401,
    [int]$TransportPort = 19402
)

$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $WorkDirectory) { throw 'Use a fresh work directory' }
New-Item -ItemType Directory -Path (Join-Path $WorkDirectory 'config') -Force | Out-Null
$store = Join-Path $WorkDirectory 'store'
New-Item -ItemType Directory -Path $store | Out-Null
$savedIp = $env:POD_IP
$savedName = $env:POD_NAME
$process = $null
try {
    $leafCertificate = [System.Security.Cryptography.X509Certificates.X509Certificate2]::CreateFromPem([System.IO.File]::ReadAllText((Join-Path $TlsDirectory 'node.crt')))
    $leafHash = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData($leafCertificate.RawData)).ToLowerInvariant()
    $trustHash = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $TlsDirectory 'ca.crt')).Hash.ToLowerInvariant()
    $arguments = @(
        '--add-opens=java.base/java.lang=ALL-UNNAMED', '--add-opens=java.base/java.nio=ALL-UNNAMED',
        '--add-opens=java.base/java.util=ALL-UNNAMED', '-Dio.netty.noUnsafe=true',
        '-Dio.netty.noKeySetOptimization=true', '-Dio.netty.recycler.maxCapacityPerThread=0',
        '-Dopensearch.set.netty.runtime.available.processors=false',
        "-Dopensearch.path.conf=$WorkDirectory/config", '-Dnode.name=https-probe-node',
        '-Dcluster.name=operator-https-smoke', "-Dpath.home=$WorkDirectory", '-Dnetwork.host=127.0.0.1',
        "-Dhttp.port=$HttpPort", "-Dtransport.port=$TransportPort", '-Dserverless.roles=ingest',
        "-Dserverless.store.path=$store", '-Dserverless.transport.require_secure=true',
        '-Dserverless.plugins=org.opensearch.serverless.operator.PeerTlsPlugin,org.opensearch.serverless.operator.HttpTlsPlugin',
        "-Dserverless.peer_tls.certificate=$TlsDirectory/node.crt", "-Dserverless.peer_tls.private_key=$TlsDirectory/node.key",
        "-Dserverless.peer_tls.trust=$TlsDirectory/ca.crt", "-Dserverless.http_tls.certificate=$TlsDirectory/node.crt",
        "-Dserverless.peer_tls.expected_leaf_sha256=$leafHash", "-Dserverless.peer_tls.expected_trust_sha256=$trustHash",
        "-Dserverless.http_tls.private_key=$TlsDirectory/node.key", "-Dserverless.http_tls.client_trust=$TlsDirectory/ca.crt",
        '-Dserverless.http_tls.require_client_certificate=true',
        '-cp', (Join-Path $RuntimeLib '*'), 'org.opensearch.serverless.shell.ServerlessBootstrap'
    )
    $process = Start-Process java -ArgumentList $arguments -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $WorkDirectory 'stdout.log') `
        -RedirectStandardError (Join-Path $WorkDirectory 'stderr.log')
    $env:POD_IP = '127.0.0.1'
    $env:POD_NAME = 'https-probe-node'
    $common = @('-role', 'ingest', '-port', "$HttpPort", '-tls-ca', (Join-Path $TlsDirectory 'ca.crt'))
    $client = @('-tls-client-cert', (Join-Path $TlsDirectory 'node.crt'), '-tls-client-key', (Join-Path $TlsDirectory 'node.key'))
    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        & $HealthProbe @common '-tls-server-name-from-pod-ip' @client 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        if ($process.HasExited) { throw 'Java exited before the HTTPS probe passed; inspect the process logs' }
        Start-Sleep -Seconds 1
    }
    if (-not $ready) { throw 'Java HTTPS readiness and lease probe did not pass' }
    & $HealthProbe @common '-tls-server-name' 'wrong.invalid' @client 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { throw 'The probe accepted the wrong HTTPS server identity' }
    & $HealthProbe @common '-tls-server-name' '127.0.0.1' 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { throw 'HTTPS accepted a probe without a client certificate' }
    $rogue = @('-tls-client-cert', (Join-Path $TlsDirectory 'rogue.crt'), '-tls-client-key', (Join-Path $TlsDirectory 'rogue.key'))
    & $HealthProbe @common '-tls-server-name' '127.0.0.1' @rogue 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { throw 'HTTPS accepted a probe with an untrusted client certificate' }
    Write-Output 'Java HTTPS readiness and lease passed; wrong server identity and missing or untrusted client certificates were rejected.'
} finally {
    if ($null -ne $process -and -not $process.HasExited) { Stop-Process -Id $process.Id; $process.WaitForExit() }
    $env:POD_IP = $savedIp
    $env:POD_NAME = $savedName
}
