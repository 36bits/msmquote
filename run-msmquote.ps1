param (
    [Parameter(Mandatory=$true)][string]$mnyFile,
    [Parameter(Mandatory=$true)][string]$mnyPswd,
    [Parameter(Mandatory=$true)][string[]]$source
)

if ($PSVersionTable.PSVersion.Major -ne 5) {
    Write-Host "This script requires PowerShell 5"
    return
}

$mnyFile = (Get-Item $mnyFile).FullName
$log = $env:LOCALAPPDATA + "\log\msmquote.log"
$jre = $env:LOCALAPPDATA + "\Programs\msmquote\bin\java.exe"
$jar = $env:LOCALAPPDATA + "\Programs\msmquote\msmquote-4.1.3.jar"
$jreOpts = @()

$duration = Measure-Command -Expression { & $jre $jreOpts -jar $jar $mnyFile $mnyPswd $source | Out-File -Append $log }
$logMsg = -join ("`nFile = ", $mnyFile, "`nExit code = ", $LASTEXITCODE, "`nDuration = ", $duration)

if ($LASTEXITCODE -eq 1) {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Warning -EventId 1001 -Category 0 -Message "Update completed with warnings.$logMsg"
} elseif ($LASTEXITCODE -eq 2) {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Error -EventId 1001 -Category 0 -Message "Update completed with errors.$logMsg"
} else {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Information -EventId 1001 -Category 0 -Message "Update completed successfully.$logMsg"
}