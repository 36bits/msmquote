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
$jar = $env:LOCALAPPDATA + "\Programs\msmquote-4.1.0.jar"
$jreOpts = @()

& $jre $jreOpts -jar $jar $mnyFile $mnyPswd $source *>> $log
$savedExitCode = $lastexitcode

if ($savedExitCode -eq 1) {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Warning -EventId 1001 -Category 0 -Message "Update completed with warnings.`nFile = $mnyFile`nExit code = $savedExitCode"
} elseif ($savedExitCode -eq 2) {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Error -EventId 1001 -Category 0 -Message "Update completed with errors.`nFile = $mnyFile`nExit code = $savedExitCode"
} else {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Information -EventId 1001 -Category 0 -Message "Update completed successfully.`nFile = $mnyFile`nExit code = $savedExitCode"
}