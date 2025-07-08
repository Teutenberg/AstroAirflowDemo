# ============================================================================
# activate_venv.ps1
#
# This script automates the setup and activation of two Python virtual environments:
#   - .venv_airflow: for Apache Airflow development (requirements.txt installed)
#   - .venv_dbt: for dbt (data build tool) development (dbt-core and dbt-postgres installed)
#
# Usage:
#   . .\activate_venv.ps1
#
# What it does:
#   1. Creates .venv_airflow and .venv_dbt if they do not exist.
#   2. Installs/upgrades pip in both environments.
#   3. Installs Airflow dependencies from requirements.txt into .venv_airflow.
#   4. Installs dbt-core and dbt-postgres into .venv_dbt.
#   5. Prints instructions for activating the environments in new terminals.
#
# Note: To activate a venv in a new terminal, use:
#   . .venv_airflow\Scripts\Activate.ps1
#   . .venv_dbt\Scripts\Activate.ps1
# ============================================================================
# Create .venv_airflow if it does not exist
$rootPath = $PSScriptRoot
$dagsPath = Join-Path $rootPath "dags"
$dbtPath = Join-Path $dagsPath "dbt"

$airflowVenv = Join-Path $PSScriptRoot ".venv_airflow"
$dbtVenv = Join-Path $PSScriptRoot ".venv_dbt"
# This is where dbt will look for the profiles.yml file
$env:DBT_PROFILES_DIR = $dbtPath

if (-not (Test-Path $airflowVenv)) {
    python -m venv $airflowVenv
    Write-Host "Created virtual environment: $airflowVenv"
} else {
    Write-Host "Found exisitng virtual environment '$airflowVenv'..." -ForegroundColor DarkGreen
}

# Create .venv_dbt if it does not exist
if (-not (Test-Path $dbtVenv)) {
    python -m venv $dbtVenv
    Write-Host "Created virtual environment: $dbtVenv"
} else {
    Write-Host "Found exisitng virtual environment '$dbtVenv'..."  -ForegroundColor DarkGreen
}

# Activate .venv_airflow and install requirements if present
Write-Host "Activating .venv_airflow and installing requirements..."  -ForegroundColor DarkGreen
& (Join-Path $airflowVenv "Scripts\Activate.ps1")
python -m pip install --upgrade pip
# Install Apache Airflow and any additional requirements from requirements.txt for linting
python -m pip install apache-airflow
if (Test-Path (Join-Path $rootPath "requirements.txt")) {
    python -m pip install -r (Join-Path $rootPath "requirements.txt")
} else {
    Write-Host "requirements.txt not found!!!" -ForegroundColor Red
    exit 1
}

# Activate .venv_dbt and install dbt dependencies
Write-Host "Activating .venv_dbt and installing dbt dependencies..."  -ForegroundColor DarkGreen
& (Join-Path $dbtVenv "Scripts\Activate.ps1")
python -m pip install --upgrade pip
if (Test-Path (Join-Path $rootPath "requirements.venv_dbt.txt")) {
    python -m pip install -r (Join-Path $rootPath "requirements.venv_dbt.txt")
} else {
    Write-Host "requirements.venv_dbt.txt not found!!!" -ForegroundColor Red
    exit 1
}

Clear-Host
# Print completion message
Write-Host ".venv_airflow is setup for dag python linting in vscode workspace config: .vscode\settings.json" -ForegroundColor DarkGreen
Write-Host ".venv_dbt is activated in terminal for dbt development." -ForegroundColor DarkGreen
Write-Host "Exec after requirements.txt updates. Happy coding!" -ForegroundColor DarkMagenta
