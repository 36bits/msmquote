param (
    [Parameter(Mandatory=$true)][string]$mnyfile,
    [Parameter(Mandatory=$false)][string]$mnypswd,
    [Parameter(Mandatory=$true)][string]$url
)


$log = $env:USERPROFILE + "\scripts\log\msmquote.log"
#$javaopts = "-showversion"
$javaopts = ""
$jar = $env:ProgramFiles + "\msmquote\msmquote.jar"
$class = "uk.co.pueblo.msmquote.OnlineUpdate"

Write-Eventlog -LogName Application -Source msmquote -EntryType Information -EventId 1000 -Category 0 -Message "Update started. File: $mnyfile."
java $javaopts -cp $jar $class $mnyfile $mnypswd $url *>> $log
$saved_exitcode = $lastexitcode

if ($saved_exitcode -eq 1) {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Warning -EventId 1001 -Category 0 -Message "Update completed with warnings (exit code $saved_exitcode). Please review the log file: $log."
} elseif ($saved_exitcode -eq 2) {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Error -EventId 1001 -Category 0 -Message "Update completed with errors (exit code $saved_exitcode). Please review the log file: $log."
} else {
    Write-Eventlog -LogName Application -Source msmquote -EntryType Information -EventId 1001 -Category 0 -Message "Update completed successfully."
}

