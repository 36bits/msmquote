param (
    [Parameter(Mandatory = $true)][string]$mnyFile,
    [Parameter(Mandatory = $true)][string]$mnyPswd,
    [Parameter(Mandatory = $false)][string[]]$source,
    [Parameter(Mandatory = $false)][boolean]$winLog = $false
)

$mnyFile = (Get-Item $mnyFile).FullName
$log = $env:LOCALAPPDATA + "\log\msmquote.log"
$jre = $env:LOCALAPPDATA + "\Programs\msmquote\bin\java.exe"
$jar = $env:LOCALAPPDATA + "\Programs\msmquote\msmquote-4.1.4.jar"
$jreOpts = @()

if ($winLog) {
    $duration = Measure-Command -Expression { & $jre $jreOpts -jar $jar $mnyFile $mnyPswd $source | Out-File -Append $log }
    if ($LASTEXITCODE -eq 2) {
        New-WinEvent -ProviderName msmquote -Id 1001 -Payload @($mnyFile, $LASTEXITCODE, $duration)
    }
    elseif ($LASTEXITCODE -eq 3) {
        New-WinEvent -ProviderName msmquote -Id 1002 -Payload @($mnyFile, $LASTEXITCODE, $duration)
    }
    else {
        New-WinEvent -ProviderName msmquote -Id 1000 -Payload @($mnyFile, $LASTEXITCODE, $duration)
    }
}
else {
    & $jre $jreOpts -jar $jar $mnyFile $mnyPswd $source | Out-File -Append $log
}