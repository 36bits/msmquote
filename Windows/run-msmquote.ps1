param (
    [Parameter(Mandatory = $true)][string]$mnyFile,
    [Parameter(Mandatory = $true)][string]$mnyPswd,
    [Parameter(Mandatory = $false)][string[]]$source
)

$mnyFile = (Get-Item $mnyFile).FullName
$log = $env:LOCALAPPDATA + "\log\msmquote.log"
$jre = $env:LOCALAPPDATA + "\Programs\msmquote\bin\java.exe"
$jar = $env:LOCALAPPDATA + "\Programs\msmquote\msmquote-4.1.7.jar"
$jreOpts = @()

if (Get-WinEvent -LogName msmquote/Operational -ErrorAction SilentlyContinue) {
    $duration = Measure-Command -Expression { & $jre $jreOpts -jar $jar $mnyFile $mnyPswd $source | Out-File -Append $log -Encoding utf8 }
    if ($LASTEXITCODE -eq 1) {
        New-WinEvent -ProviderName msmquote -Id 1001 -Payload @($mnyFile, $LASTEXITCODE, $duration)     # warnings
    }
    elseif ($LASTEXITCODE -eq 2) {
        New-WinEvent -ProviderName msmquote -Id 1002 -Payload @($mnyFile, $LASTEXITCODE, $duration)     # errors
    }
    elseif ($LASTEXITCODE -eq 3) {
        New-WinEvent -ProviderName msmquote -Id 1003 -Payload @($mnyFile, $LASTEXITCODE, $duration)     # critical errors
    }
    else {
        New-WinEvent -ProviderName msmquote -Id 1000 -Payload @($mnyFile, $LASTEXITCODE, $duration)     # OK
    }
}
else {
    & $jre $jreOpts -jar $jar $mnyFile $mnyPswd $source | Out-File -Append $log -Encoding utf8
}
