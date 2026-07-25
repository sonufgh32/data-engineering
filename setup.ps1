$ErrorActionPreference = "Stop"

# Directory where setup.ps1 is located
$ProjectDir = $PSScriptRoot

# Create pyspark\packages
$PackageDir = Join-Path $ProjectDir "pyspark\packages"
New-Item -ItemType Directory -Force -Path $PackageDir | Out-Null

# Files to download
$Downloads = @(
    @{
        Url  = "https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz"
        File = "spark-3.5.1-bin-hadoop3.tgz"
    },
    @{
        Url  = "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
        File = "awscli-exe-linux-x86_64.zip"
    },
    @{
        Url  = "https://github.com/prometheus/node_exporter/releases/download/v1.8.2/node_exporter-1.8.2.linux-amd64.tar.gz"
        File = "node_exporter-1.8.2.linux-amd64.tar.gz"
    },
    @{
        Url  = "https://github.com/grafana/alloy/releases/download/v1.11.3/alloy-linux-amd64.zip"
        File = "alloy-linux-amd64.zip"
    }
)

foreach ($Download in $Downloads) {

    $Destination = Join-Path $PackageDir $Download.File

    if (Test-Path $Destination) {
        Write-Host "Already exists: $($Download.File)"
        continue
    }

    Write-Host "Downloading $($Download.File)..."

    Invoke-WebRequest `
        -Uri $Download.Url `
        -OutFile $Destination

    Write-Host "Downloaded: $($Download.File)"
}

Write-Host ""
Write-Host "All downloads completed."
Write-Host "Location: $PackageDir"

# ------------------------------------------------------------------
# Download Maven dependencies
# ------------------------------------------------------------------

$JarDir = Join-Path $ProjectDir "pyspark\spark-jars"

New-Item -ItemType Directory -Force -Path $JarDir | Out-Null

Write-Host ""
Write-Host "Downloading Maven dependencies..."

Push-Location $ProjectDir

mvn dependency:copy-dependencies `
    -DincludeScope=runtime `
    -DoutputDirectory="$JarDir"

if ($LASTEXITCODE -ne 0) {
    Pop-Location
    throw "Failed to download Maven dependencies."
}

Pop-Location

Write-Host "Maven dependencies downloaded to:"
Write-Host $JarDir