
<#PSScriptInfo

.VERSION 1.0.0

.GUID 895933f5-356e-4a39-9b98-c2f2df5cd211

.AUTHOR jonathan@pueblo.co.uk

.COMPANYNAME

.COPYRIGHT

.TAGS

.LICENSEURI

.PROJECTURI

.ICONURI

.EXTERNALMODULEDEPENDENCIES 

.REQUIREDSCRIPTS

.EXTERNALSCRIPTDEPENDENCIES

.RELEASENOTES


.PRIVATEDATA

#>

<# 

.DESCRIPTION 
 Run msmquote 

#> 

param (
    [Parameter(Mandatory = $true)][string]$mnyFile,
    [Parameter(Mandatory = $true)][string]$mnyPswd,
    [Parameter(Mandatory = $false)][string[]]$source,
    [Parameter(Mandatory = $false)][string]$log = $env:LOCALAPPDATA + "\log\msmquote.log"
)

$mnyFile = (Get-Item $mnyFile).FullName
$jre = $env:LOCALAPPDATA + "\Programs\msmquote\bin\java.exe"
$jar = $env:LOCALAPPDATA + "\Programs\msmquote\msmquote-4.5.0.jar"
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
