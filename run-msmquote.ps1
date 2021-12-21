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
$log = $env:UserProfile + "\log\msmquote.log"
$jre = $env:ProgramFiles + "\msmquote\bin\java.exe"
$jar = $env:ProgramFiles + "\msmquote\msmquote-4.0.2.jar"
$jreOpts = @()

Write-Eventlog -LogName Application -Source msmquote -EntryType Information -EventId 1000 -Category 0 -Message "Update started: file = $mnyFile."
& $jre $jreOpts -jar $jar $mnyFile $mnyPswd $source *>> $log
$savedExitCode = $lastexitcode

if ($savedExitCode -eq 1) {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Warning -EventId 1001 -Category 0 -Message "Update completed with warnings: file = $mnyFile, exit code = $savedExitCode."
} elseif ($savedExitCode -eq 2) {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Error -EventId 1001 -Category 0 -Message "Update completed with errors: file = $mnyFile, exit code = $savedExitCode."
} else {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Information -EventId 1001 -Category 0 -Message "Update completed successfully: file = $mnyFile, exit code = $savedExitCode."
}