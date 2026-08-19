[CmdletBinding()]
param(
    [string]$PythonCommand = "python",
    [switch]$WithSpark,
    [switch]$WithStreaming
)

$ErrorActionPreference = "Stop"
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$virtualEnvironment = Join-Path $repositoryRoot ".venv"

& $PythonCommand -m venv $virtualEnvironment
$venvPython = Join-Path $virtualEnvironment "Scripts\python.exe"
& $venvPython -m pip install --upgrade pip

$extras = @("dev")
if ($WithSpark) { $extras += "spark" }
if ($WithStreaming) { $extras += "streaming" }
$extrasExpression = $extras -join ","
& $venvPython -m pip install -e "$repositoryRoot[$extrasExpression]"

Write-Host "Ambiente pronto. Ative com: .\.venv\Scripts\Activate.ps1"

