$ErrorActionPreference = 'Stop'

# Regenerate the reviewable overlay from the extracted pinned checkout and its
# saved baseline. Run from the operator repository root after editing Java.
$files = @(
    @{ Name = 'SearchReadiness.java'; Path = 'serverless/shell/src/main/java/org/opensearch/serverless/node/SearchReadiness.java' },
    @{ Name = 'SearchSettings.java'; Path = 'serverless/shell/src/main/java/org/opensearch/serverless/node/SearchSettings.java' },
    @{ Name = 'SearchWarmup.java'; Path = 'serverless/shell/src/main/java/org/opensearch/serverless/warm/SearchWarmup.java' },
    @{ Name = 'ServerlessReadyHandler.java'; Path = 'serverless/shell/src/main/java/org/opensearch/serverless/rest/ServerlessReadyHandler.java' },
    @{ Name = 'SearchWarmupTests.java'; Path = 'serverless/testkit/src/test/java/org/opensearch/serverless/testkit/SearchWarmupTests.java' }
)
$patchText = [System.Text.StringBuilder]::new()
foreach ($entry in $files) {
    $before = "tools/java-baseline/$($entry.Name)"
    $after = "tools/java-source/$($entry.Path)"
    $lines = & git -c core.autocrlf=false diff --no-index -- $before $after
    if ($LASTEXITCODE -ne 1) { throw "Expected a change in $($entry.Path), got git exit $LASTEXITCODE" }
    $part = ($lines -join "`n") + "`n"
    $part = $part.Replace("a/$before", "a/$($entry.Path)").Replace("b/$after", "b/$($entry.Path)")
    [void]$patchText.Append($part)
}
[System.IO.Directory]::CreateDirectory('java/patches') | Out-Null
[System.IO.File]::WriteAllText((Join-Path (Get-Location) 'java/patches/warmup-observation.patch'), $patchText.ToString(), [System.Text.UTF8Encoding]::new($false))
