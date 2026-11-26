param(
    [Parameter(Mandatory)][string]$Kubeconfig,
    [Parameter(Mandatory)][string]$FixtureDirectory,
    [switch]$VerifyRecovery,
    [ValidateSet('BeforeEviction', 'Throttle', 'AfterDispatch', 'ReceiptWindow')][string]$Fault = 'BeforeEviction'
)

$ErrorActionPreference = 'Stop'
$kube = (Resolve-Path -LiteralPath $Kubeconfig).Path
$work = (Resolve-Path -LiteralPath $FixtureDirectory).Path

function cluster {
    $output = & kubectl --kubeconfig $kube --request-timeout=12s @args
    if ($LASTEXITCODE) { throw "kubectl $($args[0]) failed" }
    $output
}

function namespaced { cluster -n $namespace @args }

function waitFor([scriptblock]$Check, [int]$Seconds, [string]$Description) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        $result = & $Check
        if ($result) { return $result }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $Description"
}

function storeHTTPStatus {
    & kubectl --kubeconfig $kube --request-timeout=8s -n $namespace exec probe -- curl `
        --silent --max-time 4 --output /dev/null --write-out '%{http_code}' http://s3:8333/writer-drill 2>$null
}

function switchStoreSelector([string]$From, [string]$To) {
    $patchFile = Join-Path $work 'store-selector-patch.json'
    @(
        @{op='test'; path='/metadata/uid'; value=$service.metadata.uid},
        @{op='test'; path='/spec/selector/app'; value=$From},
        @{op='replace'; path='/spec/selector/app'; value=$To}
    ) | ConvertTo-Json -AsArray | Set-Content -LiteralPath $patchFile -Encoding ascii
    namespaced patch service s3 --type=json --patch-file $patchFile | Out-Null
}

function setProxyMode([string]$Mode) {
    namespaced exec s3-fault '--' /s3-fault mode $Mode | Out-Null
}

function observeJavaFault([string]$FaultName, [string]$ClientIP, [DateTime]$Since) {
    $pattern = "fault=$FaultName client=$([regex]::Escape($ClientIP)) method="
    namespaced logs s3-fault "--since-time=$($Since.ToString('yyyy-MM-ddTHH:mm:ssZ'))" | Select-String -Pattern $pattern -Raw
}

function assertRetirementHeld {
    $operation = namespaced get opensearchserverlessoperation $fixture.operation -o json | ConvertFrom-Json
    $journal = $operation.status.writerRetirement
    $pod = namespaced get pod $fixture.victim.pod -o json | ConvertFrom-Json
    $instance = namespaced get opensearchserverless catalog -o json | ConvertFrom-Json
    if ($operation.metadata.uid -ne $initial.metadata.uid -or $operation.status.phase -ne 'Running' -or
        !$journal.admissionClosed -or !$journal.headInventory -or $journal.victimGone -or
        $journal.headInventory.observedAt -ne $initial.status.writerRetirement.headInventory.observedAt -or
        $pod.metadata.uid -ne $fixture.victim.uid -or $pod.metadata.deletionTimestamp -or
        $instance.metadata.uid -ne $fixture.instanceUID -or $instance.status.activeOperation.uid -ne $operation.metadata.uid) {
        throw 'The operation, claim, inventory, or exact victim changed during the store outage'
    }
    $operation
}

function readOperation {
    namespaced get opensearchserverlessoperation $fixture.operation -o json | ConvertFrom-Json
}

function enableEviction([switch]$SkipRolloutWait) {
    $config.enableEviction = $true
    $configPatch = Join-Path $work 'enable-outage-eviction-patch.json'
    @{metadata=@{resourceVersion=$configMap.metadata.resourceVersion}; data=@{'manager.json'=($config | ConvertTo-Json -Compress)}} |
        ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $configPatch -Encoding ascii
    namespaced patch configmap writer-config --type=merge --patch-file $configPatch | Out-Null
    namespaced rollout restart deployment/operator | Out-Null
    if (!$SkipRolloutWait) { namespaced rollout status deployment/operator --timeout=70s | Out-Null }
}

function waitForEvictionHold {
    waitFor {
        $current = assertRetirementHeld
        $reason = ($current.status.conditions | Where-Object type -eq Ready).reason
        if ($current.status.writerRetirement.evictionArmed -and $reason -eq 'WriterEvictionPending') { $current }
    } 75 'an armed eviction attempt to hold while S3 is unavailable'
}

function verifyThrottling {
    try {
        $faultStart = [DateTime]::UtcNow
        setProxyMode 'throttle'
        $raw = namespaced exec probe '--' curl --silent --max-time 5 --write-out '%{http_code}' http://s3:8333/writer-drill
        if (($raw -join '') -notmatch '<Code>SlowDown</Code>.*503$') { throw 'Expected an S3 503 SlowDown response' }
        enableEviction
        $held = waitForEvictionHold
        $javaFault = waitFor {
            observeJavaFault 'SlowDown' $survivor.status.podIP $faultStart
        } 20 'a throttled S3 request from the original surviving Java Pod'
        if ($namespace -eq 'operator-writer-s3-receipt-window') {
            # Allow the original lease to lapse so the fixture exercises a smaller head inventory.
            $remaining = 45 - ([DateTime]::UtcNow - $faultStart).TotalSeconds
            if ($remaining -gt 0) { Start-Sleep -Seconds ([Math]::Ceiling($remaining)) }
        }
        $null = assertRetirementHeld
        $holdAt = [DateTime]::UtcNow
    } finally {
        setProxyMode 'healthy'
    }
    if ((storeHTTPStatus) -ne '200') { throw 'S3 is still throttling the fixture' }
    @{operationUID=$held.metadata.uid; victimUID=$fixture.victim.uid; faultStart=$faultStart; holdConfirmedAt=$holdAt;
        restoredAt=[DateTime]::UtcNow; response=503; code='SlowDown'; javaRequests=@($javaFault);
        readyReason=($held.status.conditions | Where-Object type -eq Ready).reason} |
        ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $work 'store-throttle-hold.json') -Encoding ascii
    Write-Host 'The surviving JVM received S3 503 SlowDown responses. The operator held the armed eviction and retained the claim.'
}

function verifyPostDispatchOutage {
    if ($initial.status.writerRetirement.victimGone) {
        if (!$config.enableEviction -or !$initial.status.writerRetirement.evictionArmed -or
            $initial.status.phase -ne 'Running' -or $initial.status.writerRetirement.successors.Count -gt 0) {
            throw 'Expected an armed retirement with the victim gone and successor verification pending'
        }
    } else {
        $null = assertRetirementHeld
        if (!$config.enableEviction) { enableEviction -SkipRolloutWait }
    }
    Set-Content -LiteralPath (Join-Path $work 'watching-eviction.txt') -Value 'Watching exact victim' -Encoding ascii
    $dispatch = waitFor {
        $podJSON = namespaced get pod $fixture.victim.pod --ignore-not-found -o json
        if ($podJSON) {
            $pod = $podJSON | ConvertFrom-Json
            if ($pod.metadata.uid -ne $fixture.victim.uid) { throw 'The victim Pod identity changed' }
            if ($pod.metadata.deletionTimestamp) { @{deletionTimestamp=$pod.metadata.deletionTimestamp; observedAt=[DateTime]::UtcNow} }
        } else {
            $operation = readOperation
            if ($operation.metadata.uid -eq $initial.metadata.uid -and $operation.status.writerRetirement.victimGone) {
                @{victimGone=$true; observedAt=[DateTime]::UtcNow}
            }
        }
    } 115 'dispatch of the operator eviction for the exact Pod'

    try {
        $faultStart = [DateTime]::UtcNow
        $holdUntil = $faultStart.AddSeconds(12)
        setProxyMode 'disconnect'
        if ((storeHTTPStatus) -ne '000') { throw 'Expected S3 connections to close without an HTTP response' }
        $javaFault = waitFor {
            observeJavaFault 'Disconnect' $survivor.status.podIP $faultStart
        } 55 'a disconnected S3 request from the original surviving Java Pod'
        $held = waitFor {
            $operation = readOperation
            $instance = namespaced get opensearchserverless catalog -o json | ConvertFrom-Json
            if ($operation.metadata.uid -ne $initial.metadata.uid -or $operation.status.phase -ne 'Running' -or
                $operation.status.writerRetirement.successors.Count -gt 0 -or $instance.metadata.uid -ne $fixture.instanceUID -or
                $instance.status.activeOperation.uid -ne $operation.metadata.uid) {
                throw 'Retirement did not retain the claim while S3 was unavailable after dispatch'
            }
            if ($operation.status.writerRetirement.victimGone -and [DateTime]::UtcNow -ge $holdUntil) { $operation }
        } 85 'victim absence with the retirement still pending during the outage'
        if ((storeHTTPStatus) -ne '000') { throw 'S3 recovered before the post-eviction hold was confirmed' }
        $holdAt = [DateTime]::UtcNow
    } finally {
        setProxyMode 'healthy'
    }
    if ((storeHTTPStatus) -ne '200') { throw 'S3 connections are still failing' }
    @{operationUID=$held.metadata.uid; victimUID=$fixture.victim.uid; dispatch=$dispatch; failedAt=$faultStart;
        holdConfirmedAt=$holdAt; restoredAt=[DateTime]::UtcNow; javaRequests=@($javaFault);
        receiptRecordedAtFaultStart=[bool]$initial.status.writerRetirement.releaseReceipt;
        readyReason=($held.status.conditions | Where-Object type -eq Ready).reason; acknowledgedWrite=$ack.ack} |
        ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $work 'store-outage-hold.json') -Encoding ascii
    Write-Host 'S3 connections failed after eviction dispatch. The operation observed the victim gone and held its claim until recovery.'
}

function verifyReceiptWindowOutage {
    $null = assertRetirementHeld
    $victim = namespaced get pod $fixture.victim.pod -o json | ConvertFrom-Json
    if (!$initial.status.writerRetirement.evictionArmed -or !$config.enableEviction -or
        $victim.metadata.uid -ne $fixture.victim.uid -or !$victim.status.podIP -or
        $initial.status.writerRetirement.releaseReceipt -or $initial.status.writerRetirement.receiptMissingAt) {
        throw 'Expected the exact writer awaiting eviction with no receipt observation'
    }

    $hostName = "$($survivor.spec.hostname).$($fixture.peerSuffix)"
    $headPath = "/_serverless/writer_heads/$($fixture.victim.node)/$($fixture.victim.ephemeral)"
    $query = 'curl -fsS --max-time 8 --cacert /jvm/ca.crt --resolve "$1:9200:$2" --user "handoff-reader:$(cat /reader/password)" "https://$1:9200$3"'
    $rescanned = waitFor {
        $sample = & kubectl --kubeconfig $kube --request-timeout=12s -n $namespace exec probe -- sh -c $query shell $hostName $survivor.status.podIP $headPath 2>$null
        if (!$LASTEXITCODE -and $sample) {
            $current = $sample | ConvertFrom-Json
            if ($current.version -eq 1 -and $current.node_id -ceq $fixture.victim.node -and
                $current.ephemeral_id -ceq $fixture.victim.ephemeral -and !$current.after -and !$current.next_after -and
                $null -ne $current.heads -and $current.heads.Count -eq 0) { $current }
        }
    } 70 'a complete rescan showing the original JVM no longer owns its journaled head'
    if ($initial.status.writerRetirement.headInventory.heads.Count -ne 1) { throw 'Expected the original one-head journal' }
    $rescanned | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $work 'store-throttle-recovered-inventory.json') -Encoding ascii

    try {
        $armedAt = [DateTime]::UtcNow
        setProxyMode 'disconnect-on-receipt'
        Set-Content -LiteralPath (Join-Path $work 'watching-eviction.txt') -Value 'Armed S3 shutdown-receipt trigger' -Encoding ascii
        $trigger = waitFor {
            observeJavaFault 'ReceiptWriteDisconnect' $victim.status.podIP $armedAt
        } 110 'the exact evicted writer to attempt its S3 receipt PUT'
        $held = waitFor {
            $operation = readOperation
            $journal = $operation.status.writerRetirement
            $instance = namespaced get opensearchserverless catalog -o json | ConvertFrom-Json
            if ($operation.metadata.uid -ne $initial.metadata.uid -or $operation.status.phase -ne 'Running' -or
                $journal.releaseReceipt -or $journal.receiptMissingAt -or $journal.successors.Count -gt 0 -or
                ($journal.headInventory.heads | ConvertTo-Json -Compress) -cne ($initial.status.writerRetirement.headInventory.heads | ConvertTo-Json -Compress) -or
                $instance.metadata.uid -ne $fixture.instanceUID -or $instance.status.activeOperation.uid -ne $operation.metadata.uid) {
                throw 'Retirement changed its recovery obligations or recorded a receipt outcome during the outage'
            }
            $release = $operation.status.conditions | Where-Object type -eq WriterReleaseReady
            if ($journal.victimGone -and $release.reason -eq 'ReceiptUnavailable') { $operation }
        } 100 'victim disappearance and a held receipt readback'
        if ((storeHTTPStatus) -ne '000') { throw 'The S3 connection did not stay interrupted through receipt readback' }
        $holdAt = [DateTime]::UtcNow
        $javaFault = @(observeJavaFault 'Disconnect' $survivor.status.podIP $armedAt)
    } finally {
        setProxyMode 'healthy'
    }
    if ((storeHTTPStatus) -ne '200') { throw 'The fixture store did not recover' }
    @{operationUID=$held.metadata.uid; victimUID=$fixture.victim.uid; trigger=@($trigger); armedAt=$armedAt;
        holdConfirmedAt=$holdAt; restoredAt=[DateTime]::UtcNow; javaRequests=$javaFault;
        receiptRecordedAtFaultStart=$false; releaseReason='ReceiptUnavailable'; acknowledgedWrite=$ack.ack} |
        ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $work 'store-outage-hold.json') -Encoding ascii
    Write-Host 'The evicted writer triggered the S3 disconnect during receipt publication. Receipt readback held the original claim and inventory.'
}

function envoyRequest([string]$Method, [string]$URL, [string]$Body) {
    $curl = 'curl -fsS --max-time 8 --cacert /edge-ca/ca.pem --user "edge-user:$(cat /edge/password)"'
    if ($Method -eq 'PUT') { $curl += ' -X PUT -H "Content-Type: application/json" --data-binary "$2"' }
    $curl += ' "$1"'
    $response = & kubectl --kubeconfig $kube --request-timeout=12s -n $namespace exec probe -- sh -c $curl shell $URL $Body 2>$null
    if (!$LASTEXITCODE -and $response) { $response | ConvertFrom-Json }
}

function verifyEnvoyDocuments([string]$SuccessorDocument, [string]$SuccessorMessage) {
    $writeURL = "$($fixture.indexURL)/writer-single-shard/_doc/${SuccessorDocument}?refresh=true"
    $body = @{message=$SuccessorMessage} | ConvertTo-Json -Compress
    $newWrite = waitFor {
        $result = envoyRequest 'PUT' $writeURL $body
        if ($result._id -eq $SuccessorDocument -and $result._index -eq $fixture.seed._index -and
            $result._shard -eq $fixture.seed._shard -and $result._node -eq $fixture.survivor.node -and
            $result._primary_term -gt $fixture.seed._primary_term -and $result._shards.successful -gt 0) { $result }
    } 120 'a newer-term write through the authenticated HTTPS index route'
    $expected = @{seed='seed-before-retirement'; unpublished='unpublished-wal-before-crash'; $SuccessorDocument=$SuccessorMessage}
    $hits = @{}
    foreach ($id in $expected.Keys) {
        $hits[$id] = waitFor {
            $result = envoyRequest 'GET' "https://$($fixture.searchHost)/writer-single-shard/_search?q=_id:$id" ''
            if ($result.hits.total.value -eq 1 -and $result.hits.hits[0]._id -eq $id -and
                $result.hits.hits[0]._source.message -ceq $expected[$id]) { $result.hits.hits[0] }
        } 35 "the authenticated HTTPS search route to return $id"
    }
    @{write=$newWrite; search=$hits}
}

function verifyThrottleRecovery {
    $hold = Get-Content -Raw -LiteralPath (Join-Path $work 'store-throttle-hold.json') | ConvertFrom-Json
    $current = readOperation
    if ($current.metadata.uid -ne $hold.operationUID -or (storeHTTPStatus) -ne '200') {
        throw 'Expected the same retirement and a recovered S3 endpoint'
    }
    $traffic = verifyEnvoyDocuments 'after-store-throttle' 'successor-after-store-throttle'
    $current = readOperation
    $instance = namespaced get opensearchserverless catalog -o json | ConvertFrom-Json
    if ($current.metadata.uid -ne $hold.operationUID -or $instance.metadata.uid -ne $fixture.instanceUID) {
        throw 'Retirement or instance identity changed during readback'
    }
    @{throttle=$hold; successorWrite=$traffic.write; search=$traffic.search; phase=$current.status.phase;
        victimGone=[bool]$current.status.writerRetirement.victimGone;
        readyReason=($current.status.conditions | Where-Object type -eq Ready).reason;
        claimHeld=$instance.status.activeOperation.uid -eq $hold.operationUID} |
        ConvertTo-Json -Depth 14 | Set-Content -LiteralPath (Join-Path $work 'store-throttle-result.json') -Encoding ascii
    Write-Host 'The survivor accepted a newer-term write and Envoy search returned all three documents after throttling.'
}

function verifyRecovery {
    $hold = Get-Content -Raw -LiteralPath (Join-Path $work 'store-outage-hold.json') | ConvertFrom-Json
    $gone = waitFor {
        $operation = readOperation
        if ($operation.metadata.uid -ne $hold.operationUID -or $operation.status.phase -ne 'Running') {
            throw 'Expected the same running retirement before the successor write'
        }
        if ($operation.status.writerRetirement.victimGone) { $operation }
    } 120 'operator-confirmed removal of the retiring writer'
    $instance = namespaced get opensearchserverless catalog -o json | ConvertFrom-Json
    $oldPod = namespaced get pod $fixture.victim.pod --ignore-not-found -o json
    if ($instance.metadata.uid -ne $fixture.instanceUID -or $instance.status.activeOperation.uid -ne $gone.metadata.uid -or
        ($oldPod -and ($oldPod | ConvertFrom-Json).metadata.uid -eq $fixture.victim.uid)) {
        throw 'The original Pod remains or the operation released its claim before successor write evidence'
    }

    $traffic = verifyEnvoyDocuments 'after-store-outage' 'successor-after-store-outage'
    $newWrite = $traffic.write

    $done = waitFor {
        $operation = readOperation
        $instance = namespaced get opensearchserverless catalog -o json | ConvertFrom-Json
        if ($operation.metadata.uid -ne $hold.operationUID -or $operation.status.phase -eq 'Failed' -or
            $instance.metadata.uid -ne $fixture.instanceUID) { throw 'Retirement failed or changed identity' }
        if ($operation.status.phase -eq 'Succeeded' -and !$instance.status.activeOperation) { $operation }
    } 120 'terminal Succeeded and claim release'
    $successors = @($done.status.writerRetirement.successors)
    if ($successors.Count -ne 1 -or $successors[0].podUID -ne $fixture.survivor.uid -or
        $successors[0].nodeID -ne $fixture.survivor.node -or $successors[0].ephemeralID -ne $fixture.survivor.ephemeral -or
        $successors[0].term -ne $newWrite._primary_term) { throw 'The terminal journal did not verify the exact successor' }
    if (!$done.status.writerRetirement.releaseReceipt -and !$successors[0].predecessorWALSettled) {
        throw 'Expected either a release receipt or positive predecessor WAL settlement evidence'
    }
    if ($Fault -eq 'ReceiptWindow' -and ($done.status.writerRetirement.releaseReceipt -or
        !$done.status.writerRetirement.receiptMissingAt -or !$successors[0].predecessorWALSettled)) {
        throw 'Expected confirmed receipt absence after restoration and durable WAL settlement on the successor'
    }

    @{outage=$hold; successorWrite=$newWrite; successorProof=$successors[0]; search=$traffic.search;
        receiptRecorded=[bool]$done.status.writerRetirement.releaseReceipt;
        receiptMissingAt=$done.status.writerRetirement.receiptMissingAt; phase=$done.status.phase; claimReleased=$true} |
        ConvertTo-Json -Depth 14 | Set-Content -LiteralPath (Join-Path $work 'store-outage-result.json') -Encoding ascii
    Write-Host 'The operator evicted the exact writer, verified the newer-term write, and released the claim. Envoy search returned all three documents.'
}

if ((cluster config current-context) -ne 'kind-oss-envoy-rerun' -or
    (cluster get nodes -o name) -ne 'node/oss-envoy-rerun-control-plane') {
    throw 'Expected the disposable writer test cluster'
}
$fixture = Get-Content -Raw -LiteralPath (Join-Path $work 'fixture.json') | ConvertFrom-Json
$namespace = $fixture.namespace
$expectedNamespace = switch ($Fault) {
    'BeforeEviction' { 'operator-writer-store-outage' }
    'Throttle' { 'operator-writer-s3-faults'; 'operator-writer-s3-receipt-window' }
    'AfterDispatch' { 'operator-writer-s3-post-dispatch' }
    'ReceiptWindow' { 'operator-writer-s3-receipt-window' }
}
if ($namespace -notin $expectedNamespace) { throw 'Fixture namespace does not match the selected fault' }
$ack = Get-Content -Raw -LiteralPath (Join-Path $work 'unpublished-ack-proof.json') | ConvertFrom-Json
if ($ack.ack._id -ne 'unpublished' -or $ack.ack.durable -ne 'write-ahead log' -or
    $ack.ack._node -ne $fixture.victim.node -or $ack.baseline.sha256 -ne $ack.afterAck.sha256) {
    throw 'Expected a verified WAL-only acknowledgment from the retiring fixture writer'
}
if ($VerifyRecovery) {
    if ($Fault -eq 'Throttle') { verifyThrottleRecovery } else { verifyRecovery }
    return
}
$initial = namespaced get opensearchserverlessoperation $fixture.operation -o json | ConvertFrom-Json
$service = namespaced get service s3 -o json | ConvertFrom-Json
$storeBefore = namespaced get pod s3 -o json | ConvertFrom-Json
$configMap = namespaced get configmap writer-config -o json | ConvertFrom-Json
$config = $configMap.data.'manager.json' | ConvertFrom-Json
if ($Fault -ne 'BeforeEviction') {
    $survivor = namespaced get pod $fixture.survivor.pod -o json | ConvertFrom-Json
    if ($config.gatewayNamespace -ne $namespace -or $config.instance -ne 'catalog' -or
        $initial.spec.writerRetirement.podUID -ne $fixture.victim.uid -or $survivor.metadata.uid -ne $fixture.survivor.uid -or
        !$survivor.status.podIP -or $service.spec.selector.app -ne 's3-fault' -or
        $storeBefore.metadata.labels.app -ne 's3-backend' -or (storeHTTPStatus) -ne '200') {
        throw 'Expected the isolated S3 fault proxy, exact survivor, and reachable backing store'
    }
    if ($Fault -eq 'ReceiptWindow') {
        verifyReceiptWindowOutage
    } elseif ($Fault -eq 'AfterDispatch') {
        verifyPostDispatchOutage
    } else {
        if ($config.enableEviction -ne $false -or $initial.status.writerRetirement.evictionArmed) {
            throw 'Expected automatic eviction disabled before the throttling test'
        }
        $null = assertRetirementHeld
        verifyThrottling
    }
    return
}
if ($config.gatewayNamespace -ne $namespace -or $config.instance -ne 'catalog' -or $config.enableEviction -ne $false -or
    $initial.spec.writerRetirement.podUID -ne $fixture.victim.uid -or $initial.status.writerRetirement.evictionArmed -or
    $service.spec.selector.app -ne 's3' -or $storeBefore.metadata.labels.app -ne 's3' -or (storeHTTPStatus) -ne '200') {
    throw 'Expected a reachable fixture store and a closed writer with automatic eviction disabled'
}
$null = assertRetirementHeld

try {
    switchStoreSelector 's3' 'writer-store-outage'
    $failedAt = waitFor {
        if ((storeHTTPStatus) -eq '000') { [DateTime]::UtcNow }
    } 25 'the S3 Service to become unreachable from the probe'

    enableEviction
    $held = waitForEvictionHold
    if ((storeHTTPStatus) -ne '000') { throw 'The S3 Service became reachable before the hold was verified' }
    $confirmedAt = [DateTime]::UtcNow
} finally {
    $currentService = namespaced get service s3 -o json | ConvertFrom-Json
    if ($currentService.metadata.uid -eq $service.metadata.uid -and $currentService.spec.selector.app -eq 'writer-store-outage') {
        switchStoreSelector 'writer-store-outage' 's3'
    }
}

$restoredAt = waitFor {
    if ((storeHTTPStatus) -eq '200') { [DateTime]::UtcNow }
} 30 'the original S3 Service to recover'
$storeAfter = namespaced get pod s3 -o json | ConvertFrom-Json
if ($storeAfter.metadata.uid -ne $storeBefore.metadata.uid -or
    $storeAfter.status.containerStatuses[0].containerID -ne $storeBefore.status.containerStatuses[0].containerID) {
    throw 'The S3 process changed during the outage'
}
@{failedAt=$failedAt; holdConfirmedAt=$confirmedAt; restoredAt=$restoredAt; operationUID=$held.metadata.uid;
    victimUID=$fixture.victim.uid; storePodUID=$storeAfter.metadata.uid; acknowledgedWrite=$ack.ack;
    readyReason=($held.status.conditions | Where-Object type -eq Ready).reason} |
    ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $work 'store-outage-hold.json') -Encoding ascii
Write-Host 'The armed retirement held the exact victim during S3 unavailability. The same S3 process is reachable again.'
