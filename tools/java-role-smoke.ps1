param(
    [Parameter(Mandatory = $true)][string]$RuntimeLib,
    [Parameter(Mandatory = $true)][string]$WorkDirectory,
    [int]$IngestPort = 19201,
    [int]$SearchPort = 19202,
    [string]$PeerTlsDirectory = '',
    [string]$S3Endpoint = '',
    [string]$S3Bucket = '',
    [string]$S3Keystore = ''
)

$ErrorActionPreference = 'Stop'
$store = Join-Path $WorkDirectory 'store'
if (Test-Path -LiteralPath $store) {
    throw 'Use a fresh work directory for the smoke test'
}
New-Item -ItemType Directory -Force -Path $store | Out-Null
$processes = @()
if ($env:OPERATOR_TEST_DEBUG_WARM_COPY -eq '1') {
    $loggingConfig = Join-Path $WorkDirectory 'log4j2.properties'
    @'
status = error
name = OperatorSmoke
appender.console.type = Console
appender.console.name = Console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{ISO8601} %-5level %c - %m%n
rootLogger.level = info
rootLogger.appenderRef.console.ref = Console
logger.warmcopy.name = org.opensearch.serverless.transport.CacheWarmCopy
logger.warmcopy.level = debug
logger.warm.name = org.opensearch.serverless.warm.SearchWarmup
logger.warm.level = debug
'@ | Set-Content -LiteralPath $loggingConfig -Encoding utf8
}

function Start-ShellNode([string]$Name, [int]$Port, [int]$TransportPort, [string]$Role) {
    $homePath = Join-Path $WorkDirectory $Name
    New-Item -ItemType Directory -Force -Path (Join-Path $homePath 'config') | Out-Null
    $storeArguments = @("-Dserverless.store.path=$store")
    if ($S3Endpoint) {
        if (-not $S3Bucket -or -not (Test-Path -LiteralPath $S3Keystore -PathType Leaf)) { throw 'S3 bucket and test keystore are required' }
        Copy-Item -LiteralPath $S3Keystore -Destination (Join-Path $homePath 'config/opensearch.keystore')
        $storeArguments = @('-Dserverless.store.type=s3', "-Dserverless.store.bucket=$S3Bucket", '-Dserverless.store.region=us-east-1', "-Dserverless.store.endpoint=$S3Endpoint", '-Dserverless.store.path_style_access=true')
    }
    $arguments = @(
        '--add-opens=java.base/java.lang=ALL-UNNAMED',
        '--add-opens=java.base/java.nio=ALL-UNNAMED',
        '--add-opens=java.base/java.util=ALL-UNNAMED',
        '-Dio.netty.noUnsafe=true', '-Dio.netty.noKeySetOptimization=true',
        '-Dio.netty.recycler.maxCapacityPerThread=0', '-Dopensearch.set.netty.runtime.available.processors=false',
        "-Dopensearch.path.conf=$homePath/config", "-Dnode.name=$Name", '-Dcluster.name=operator-smoke',
        "-Dpath.home=$homePath", '-Dnetwork.host=127.0.0.1', "-Dhttp.port=$Port",
        "-Dtransport.port=$TransportPort", "-Dserverless.roles=$Role"
    ) + $storeArguments + @(
        '-cp', (Join-Path $RuntimeLib '*'), 'org.opensearch.serverless.shell.ServerlessBootstrap'
    )
    if ($Role -eq 'search' -and $env:OPERATOR_TEST_WARM_ON_JOIN -eq '1') {
        $arguments = @('-Dserverless.search.warm_on_join=true') + $arguments
        if ($env:OPERATOR_TEST_WARM_MAX_WAIT) {
            $arguments = @("-Dserverless.search.warm_on_join.max_wait=$env:OPERATOR_TEST_WARM_MAX_WAIT") + $arguments
        }
    }
    if ($Name -eq 'search-warm' -and $env:OPERATOR_TEST_STRICT_WARM_JOIN -eq '1') {
        $arguments = @('-Dserverless.search.require_warmup_completion=true') + $arguments
    }
    if ($env:OPERATOR_TEST_DEBUG_WARM_COPY -eq '1') {
        $arguments = @("-Dlog4j.configurationFile=$loggingConfig") + $arguments
    }
    if ($PeerTlsDirectory) {
        $arguments = @(
            '-Dserverless.plugins=org.opensearch.serverless.operator.PeerTlsPlugin',
            '-Dserverless.transport.require_secure=true',
            "-Dserverless.peer_tls.certificate=$PeerTlsDirectory/node.crt",
            "-Dserverless.peer_tls.private_key=$PeerTlsDirectory/node.key",
            "-Dserverless.peer_tls.trust=$PeerTlsDirectory/ca.crt"
        ) + $arguments
    }
    return Start-Process java -ArgumentList $arguments -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $homePath 'stdout.log') `
        -RedirectStandardError (Join-Path $homePath 'stderr.log')
}

try {
    $processes += Start-ShellNode 'ingest' $IngestPort 19301 'ingest'
    $processes += Start-ShellNode 'search' $SearchPort 19302 'search'
    foreach ($port in @($IngestPort, $SearchPort)) {
        $ready = $false
        for ($attempt = 0; $attempt -lt 60; $attempt++) {
            try {
                $response = Invoke-RestMethod -Uri "http://127.0.0.1:$port/_serverless/ready" -TimeoutSec 2
                if ($response.ready) { $ready = $true; break }
            } catch {}
            Start-Sleep -Seconds 1
        }
        if (-not $ready) { throw "Node on port $port did not become ready" }
    }
    if ($env:OPERATOR_GO_EXE) {
        $env:SERVERLESS_TEST_ENDPOINT = "http://127.0.0.1:$IngestPort"
		$env:SERVERLESS_TEST_NODE_NAME = 'ingest'
        & $env:OPERATOR_GO_EXE test ./internal/java -run '^TestLiveReadinessAndSettings$' -count=1
        if ($LASTEXITCODE -ne 0) { throw 'Go Java adapter integration test failed' }
		if ($env:SERVERLESS_TEST_EXPECT_WARMUP -eq '1') {
			$env:SERVERLESS_TEST_CANDIDATE_INGEST = "http://127.0.0.1:$IngestPort"
			try {
				& $env:OPERATOR_GO_EXE test ./internal/controller -run '^TestLiveCandidateIngestObservation$' -count=1
				if ($LASTEXITCODE -ne 0) { throw 'Candidate ingest Java observation failed' }
			} finally { Remove-Item Env:SERVERLESS_TEST_CANDIDATE_INGEST -ErrorAction SilentlyContinue }
		}
        Remove-Item Env:SERVERLESS_TEST_ENDPOINT
		Remove-Item Env:SERVERLESS_TEST_NODE_NAME
        if ($env:OPERATOR_TEST_WARM_ON_JOIN -eq '1') {
            $env:SERVERLESS_TEST_WARM_ENDPOINT = "http://127.0.0.1:$SearchPort"
            & $env:OPERATOR_GO_EXE test ./internal/java -run '^TestLiveWarmupStatus$' -count=1
            if ($LASTEXITCODE -ne 0) { throw 'Java warm-up status integration test failed' }
            Remove-Item Env:SERVERLESS_TEST_WARM_ENDPOINT
            if ($env:OPERATOR_HEALTH_PROBE_EXE) {
                $env:POD_IP = '127.0.0.1'
                $env:POD_NAME = 'search'
                & $env:OPERATOR_HEALTH_PROBE_EXE -role search -port "$SearchPort" -minimum-lease 1s -require-warmup-completion
                if ($LASTEXITCODE -eq 0) { throw 'Degraded Java search passed warm-up completion probe' }
            }
        }
    }
    Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:$IngestPort/smoke" `
        -ContentType 'application/json' -Body '{}' | Out-Null
    Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:$IngestPort/smoke/_doc/1?refresh=true" `
        -ContentType 'application/json' -Body '{"message":"operator smoke"}' | Out-Null
    $found = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        try {
            $result = Invoke-RestMethod -Uri "http://127.0.0.1:$SearchPort/smoke/_search?q=message:operator" -TimeoutSec 3
            if ($result.hits.total.value -eq 1) { $found = $true; break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $found) { throw 'Search did not return the indexed document' }
    if ($env:OPERATOR_TEST_WARM_ON_JOIN -eq '1' -and $env:OPERATOR_GO_EXE) {
        $processes += Start-ShellNode 'search-warm' ($SearchPort + 1) 19304 'search'
        $env:SERVERLESS_TEST_QUALIFIED_ENDPOINT = "http://127.0.0.1:$($SearchPort + 1)"
        try {
            & $env:OPERATOR_GO_EXE test ./internal/java -run '^TestLiveQualifiedSearch$' -count=1
            if ($LASTEXITCODE -ne 0) { throw 'Java search qualification integration test failed' }
            $env:SERVERLESS_TEST_CANDIDATE_SEARCH = $env:SERVERLESS_TEST_QUALIFIED_ENDPOINT
            try {
                & $env:OPERATOR_GO_EXE test ./internal/controller -run '^TestLiveCandidateSearchObservation$' -count=1
                if ($LASTEXITCODE -ne 0) { throw 'Candidate search Java observation failed' }
            } finally { Remove-Item Env:SERVERLESS_TEST_CANDIDATE_SEARCH -ErrorAction SilentlyContinue }
            if ($env:OPERATOR_HEALTH_PROBE_EXE) {
                $env:POD_IP = '127.0.0.1'
                $env:POD_NAME = 'search-warm'
                & $env:OPERATOR_HEALTH_PROBE_EXE -role search -port "$($SearchPort + 1)" -minimum-lease 1s -require-warmup-completion
                if ($LASTEXITCODE -ne 0) { throw 'Warmed Java search failed warm-up completion probe' }
            }
            $beforeRestart = Invoke-RestMethod -Uri "$env:SERVERLESS_TEST_QUALIFIED_ENDPOINT/_serverless/ready" -TimeoutSec 2
            $beforeStats = Invoke-RestMethod -Uri "$env:SERVERLESS_TEST_QUALIFIED_ENDPOINT/_serverless/stats" -TimeoutSec 2
            $beforeNodes = Invoke-RestMethod -Uri "http://127.0.0.1:$SearchPort/_serverless/nodes" -TimeoutSec 2
            if (-not $beforeRestart.process_incarnation) { throw 'Search process incarnation missing before restart' }
            Stop-Process -Id $processes[2].Id
            $processes[2].WaitForExit()
            $afterStopNodes = Invoke-RestMethod -Uri "http://127.0.0.1:$SearchPort/_serverless/nodes" -TimeoutSec 2
            if ($env:OPERATOR_TEST_WAIT_LEASE_EXPIRY -eq '1') {
                $until = [DateTime]::UtcNow.AddSeconds(90)
                do {
                    $currentNodes = Invoke-RestMethod -Uri "http://127.0.0.1:$SearchPort/_serverless/nodes" -TimeoutSec 2
                    if (@($currentNodes.nodes | Where-Object { $_.node_id -eq $beforeStats.node }).Count -eq 0) { break }
                    Start-Sleep -Milliseconds 500
                } while ([DateTime]::UtcNow -lt $until)
                if (@($currentNodes.nodes | Where-Object { $_.node_id -eq $beforeStats.node }).Count -ne 0) { throw 'Prior search lease remained visible after 90 seconds' }
            }
            $processes += Start-ShellNode 'search-warm' ($SearchPort + 1) 19304 'search'
            $env:SERVERLESS_TEST_PREVIOUS_INCARNATION = $beforeRestart.process_incarnation
            if ($env:OPERATOR_TEST_WAIT_LEASE_EXPIRY -eq '1' -or ($env:OPERATOR_TEST_EXPECT_RETRY -eq '1' -and $env:OPERATOR_TEST_EXPECT_DEGRADED -ne '1')) { $env:SERVERLESS_TEST_EXPECT_COMPLETED = '1' }
            if ($env:OPERATOR_TEST_EXPECT_RETRY -eq '1') { $env:SERVERLESS_TEST_EXPECT_RETRY = '1' }
            if ($env:OPERATOR_TEST_EXPECT_DEGRADED -eq '1') { $env:SERVERLESS_TEST_EXPECT_DEGRADED = '1' }
            & $env:OPERATOR_GO_EXE test ./internal/java -run '^TestLiveRestartedSearch$' -count=1 -v
            if ($LASTEXITCODE -ne 0) { throw 'Restarted Java search identity integration test failed' }
            $afterRestartResponse = Invoke-WebRequest -Uri "$env:SERVERLESS_TEST_QUALIFIED_ENDPOINT/_serverless/ready" -TimeoutSec 2 -SkipHttpErrorCheck
            $afterRestart = $afterRestartResponse.Content | ConvertFrom-Json
            $afterStats = Invoke-RestMethod -Uri "$env:SERVERLESS_TEST_QUALIFIED_ENDPOINT/_serverless/stats" -TimeoutSec 2
            $afterNodes = Invoke-RestMethod -Uri "$env:SERVERLESS_TEST_QUALIFIED_ENDPOINT/_serverless/nodes" -TimeoutSec 2
            if ($env:OPERATOR_TEST_STRICT_WARM_JOIN -eq '1' -and $env:OPERATOR_TEST_EXPECT_DEGRADED -eq '1') {
                if ($afterRestart.ready -or $afterRestartResponse.StatusCode -ne 503) { throw 'Strict degraded search became ready' }
                $self = @($afterNodes.nodes | Where-Object { $_.node_id -eq $afterStats.node })
                if ($self.Count -ne 1 -or $self[0].roles -contains 'search') { throw 'Strict degraded search advertised the search role' }
            }
            $oldPeerRequestFailed = $null
            if ($env:OPERATOR_TEST_DEBUG_WARM_COPY -eq '1') {
                $restartLog = Join-Path $WorkDirectory 'search-warm/stderr.log'
                $oldPeerRequestFailed = [bool](Select-String -LiteralPath $restartLog -SimpleMatch "Search peer did not report its cached blocks (node_id=$($beforeStats.node))" -Quiet)
                Write-Output "Old search lease peer request failed: $oldPeerRequestFailed"
            }
            [ordered]@{ beforeReady = $beforeRestart; beforeNode = $beforeStats.node; beforeNodes = $beforeNodes; afterStopNodes = $afterStopNodes; afterReady = $afterRestart; afterNode = $afterStats.node; afterNodes = $afterNodes; oldPeerRequestFailed = $oldPeerRequestFailed } |
                ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $WorkDirectory 'search-restart-diagnostics.json')
        } finally {
            Remove-Item Env:SERVERLESS_TEST_QUALIFIED_ENDPOINT -ErrorAction SilentlyContinue
            Remove-Item Env:SERVERLESS_TEST_PREVIOUS_INCARNATION -ErrorAction SilentlyContinue
            Remove-Item Env:SERVERLESS_TEST_EXPECT_COMPLETED -ErrorAction SilentlyContinue
            Remove-Item Env:SERVERLESS_TEST_EXPECT_RETRY -ErrorAction SilentlyContinue
            Remove-Item Env:SERVERLESS_TEST_EXPECT_DEGRADED -ErrorAction SilentlyContinue
            Remove-Item Env:POD_IP -ErrorAction SilentlyContinue
            Remove-Item Env:POD_NAME -ErrorAction SilentlyContinue
        }
    }
    if ($env:OPERATOR_TEST_MIXED_FLEET -eq '1') {
        $candidatePort = $IngestPort + 3
        $candidate = Start-ShellNode 'ingest-candidate' $candidatePort 19305 'ingest'
        $processes += $candidate
        $candidateReady = $false
        for ($attempt = 0; $attempt -lt 60; $attempt++) {
            try {
                $response = Invoke-RestMethod -Uri "http://127.0.0.1:$candidatePort/_serverless/ready" -TimeoutSec 2
                if ($response.ready) { $candidateReady = $true; break }
            } catch {}
            Start-Sleep -Seconds 1
        }
        if (-not $candidateReady) { throw 'Candidate ingest process did not become ready' }
        $written = $false
        for ($attempt = 0; $attempt -lt 60; $attempt++) {
            try {
                $response = Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:$candidatePort/smoke/_doc/3?refresh=true" -ContentType 'application/json' -Body '{"message":"candidate write"}' -TimeoutSec 3
                if ($response.result -eq 'created' -or $response.result -eq 'updated') { $written = $true; break }
            } catch {}
            Start-Sleep -Seconds 1
        }
        if (-not $written) { throw 'Candidate ingest did not accept the write' }
        $readOldFleet = $false
        for ($attempt = 0; $attempt -lt 30; $attempt++) {
            try {
                $result = Invoke-RestMethod -Uri "http://127.0.0.1:$IngestPort/smoke/_doc/3" -TimeoutSec 3
                if ($result.found -and $result._source.message -eq 'candidate write') { $readOldFleet = $true; break }
            } catch {}
            Start-Sleep -Seconds 1
        }
        if (-not $readOldFleet) { throw 'Original ingest did not read the candidate write' }
        $candidateOwnedIndex = $null
        if ($env:OPERATOR_TEST_CANDIDATE_OWNS -eq '1') {
            $candidateNode = (Invoke-RestMethod -Uri "http://127.0.0.1:$candidatePort/_serverless/stats" -TimeoutSec 3).node
            if (-not $candidateNode) { throw 'Candidate node identity is missing' }
            for ($indexAttempt = 1; $indexAttempt -le 6 -and -not $candidateOwnedIndex; $indexAttempt++) {
                $index = "candidate-owned-$indexAttempt"
                Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:$candidatePort/$index" -ContentType 'application/json' -Body '{"settings":{"number_of_shards":1}}' -TimeoutSec 5 | Out-Null
                $candidateWrite = $false
                for ($attempt = 0; $attempt -lt 30; $attempt++) {
                    try {
                        $response = Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:$candidatePort/$index/_doc/1?refresh=true" -ContentType 'application/json' -Body '{"message":"candidate owned"}' -TimeoutSec 3
                        if ($response.result -eq 'created' -or $response.result -eq 'updated') { $candidateWrite = $true; break }
                    } catch {}
                    Start-Sleep -Seconds 1
                }
                if (-not $candidateWrite) { throw 'Candidate-owned index write did not succeed' }
                $shards = Invoke-RestMethod -Uri "http://127.0.0.1:$IngestPort/_serverless/shards/$index" -TimeoutSec 3
                if (@($shards.shards | Where-Object { $_.state -eq 'owned' -and $_.owner_node_id -eq $candidateNode }).Count -eq 1) { $candidateOwnedIndex = $index }
            }
            if (-not $candidateOwnedIndex) { throw 'No test shard was observed owned by the candidate' }
            $beforeHead = (Invoke-RestMethod -Uri "http://127.0.0.1:$IngestPort/_serverless/shards/$candidateOwnedIndex" -TimeoutSec 3).shards[0]
            if ($beforeHead.owner_node_id -ne $candidateNode) { throw 'Candidate lost ownership before the verified write' }
            Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:$candidatePort/$candidateOwnedIndex/_doc/2?refresh=true" -ContentType 'application/json' -Body '{"message":"verified candidate owner"}' -TimeoutSec 5 | Out-Null
            $afterHead = (Invoke-RestMethod -Uri "http://127.0.0.1:$IngestPort/_serverless/shards/$candidateOwnedIndex" -TimeoutSec 3).shards[0]
            if ($afterHead.owner_node_id -ne $candidateNode -or $afterHead.term -ne $beforeHead.term) { throw 'Shard ownership changed during the verified write' }
            $ownedRead = Invoke-RestMethod -Uri "http://127.0.0.1:$IngestPort/$candidateOwnedIndex/_doc/2" -TimeoutSec 5
            if (-not $ownedRead.found -or $ownedRead._source.message -ne 'verified candidate owner') { throw 'Original ingest could not read the candidate-owned shard' }
        }
        Stop-Process -Id $candidate.Id
        $candidate.WaitForExit()
        $readAfterStop = $false
        for ($attempt = 0; $attempt -lt 90; $attempt++) {
            try {
                $result = Invoke-RestMethod -Uri "http://127.0.0.1:$IngestPort/smoke/_doc/3" -TimeoutSec 3
                if ($result.found -and $result._source.message -eq 'candidate write') { $readAfterStop = $true; break }
            } catch {}
            Start-Sleep -Seconds 1
        }
        if (-not $readAfterStop) { throw 'Original ingest did not recover the candidate write after stop' }
        if ($candidateOwnedIndex) {
            $recoveredOwned = $false
            for ($attempt = 0; $attempt -lt 90; $attempt++) {
                try {
                    $result = Invoke-RestMethod -Uri "http://127.0.0.1:$IngestPort/$candidateOwnedIndex/_doc/2" -TimeoutSec 3
                    if ($result.found -and $result._source.message -eq 'verified candidate owner') { $recoveredOwned = $true; break }
                } catch {}
                Start-Sleep -Seconds 1
            }
            if (-not $recoveredOwned) { throw 'Original ingest could not recover the candidate-owned shard' }
            Write-Output 'Original ingest recovered a document from a candidate-owned shard after candidate stop.'
        }
        Write-Output 'Same-image candidate write remained readable through original ingest after candidate stop.'
    }
    Stop-Process -Id $processes[0].Id
    $processes[0].WaitForExit()
    $processes += Start-ShellNode 'ingest-replacement' $IngestPort 19303 'ingest'
    $recovered = $false
    for ($attempt = 0; $attempt -lt 90; $attempt++) {
        try {
            $response = Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:$IngestPort/smoke/_doc/2?refresh=true" `
                -ContentType 'application/json' -Body '{"message":"replacement write"}' -TimeoutSec 3
            if ($response.result -eq 'created' -or $response.result -eq 'updated') { $recovered = $true; break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $recovered) { throw 'Replacement ingest process could not write after the original stopped' }
    $found = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        try {
            $result = Invoke-RestMethod -Uri "http://127.0.0.1:$SearchPort/smoke/_search?q=message:operator" -TimeoutSec 3
            if ($result.hits.total.value -eq 1) { $found = $true; break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $found) { throw 'Original acknowledged document was not found after replacement' }
    Write-Output 'Separate roles indexed and searched; replacement ingest wrote and the original document remained searchable.'
} finally {
    foreach ($process in $processes) {
        if (-not $process.HasExited) { Stop-Process -Id $process.Id }
    }
}
