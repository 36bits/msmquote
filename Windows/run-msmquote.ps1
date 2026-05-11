
<#PSScriptInfo

.VERSION 1.1.0

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
    [Parameter(Mandatory = $false)][string]$logFile = (Join-Path $env:LOCALAPPDATA "Log" "msmquote.log"),
    [Parameter(Mandatory = $false)][boolean]$job = $false,
    [Parameter(Mandatory = $false)][string]$tmpDir = $env:TMP
)

$env:TMP = $tmpDir

if ($job) {
    $logFile = New-TemporaryFile
}

$jre = Join-Path $env:LOCALAPPDATA "Programs" "msmquote" "bin" "java.exe"
$jar = Join-Path $env:LOCALAPPDATA "Programs" "msmquote" "msmquote-4.13.0.jar"
$jreOpts = @()

# Escape backslashes in paths
$jar = $jar.Replace("\", "\\")
$mnyFile = (Get-Item $mnyFile).FullName.Replace("\", "\\")

# Create arguments file
$jarArg = "-jar `"$jar`" `"$mnyFile`" `"$mnyPswd`""
foreach($sourceArg in $source) {
    $jarArg = "$jarArg `"$sourceArg`""
}
$argFile = New-TemporaryFile
Write-Output $jarArg | Out-File $argFile

if (Get-WinEvent -LogName Pueblo-applications-msmquote/Operational -ErrorAction SilentlyContinue) {
    $duration = Measure-Command -Expression { & $jre $jreOpts ("@" + $argFile) | Out-File -Append $logFile -Encoding utf8 }
    if ($LASTEXITCODE -eq 1) {
        New-WinEvent -ProviderName Pueblo-applications-msmquote -Id 1001 -Payload @($mnyFile, $LASTEXITCODE, $duration)     # warnings
    }
    elseif ($LASTEXITCODE -eq 2) {
        New-WinEvent -ProviderName Pueblo-applications-msmquote -Id 1002 -Payload @($mnyFile, $LASTEXITCODE, $duration)     # errors
    }
    elseif ($LASTEXITCODE -eq 3) {
        New-WinEvent -ProviderName Pueblo-applications-msmquote -Id 1003 -Payload @($mnyFile, $LASTEXITCODE, $duration)     # critical errors
    }
    else {
        New-WinEvent -ProviderName Pueblo-applications-msmquote -Id 1000 -Payload @($mnyFile, $LASTEXITCODE, $duration)     # OK
    }
}
else {
    & $jre $jreOpts ("@" + $argFile) | Out-File -Append $logFile -Encoding utf8
}

Remove-Item $argFile

if ($job) {
    Get-Content $logFile
    Remove-Item $logFile
}