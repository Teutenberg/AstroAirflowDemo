# Create .venv_airflow if it does not exist
if (-not (Test-Path ".\.venv_airflow")) {
    python -m venv ".\.venv_airflow"
    Write-Host "Created virtual environment: .venv_airflow"
} else {
    Write-Host "Virtual environment '.venv_airflow' already exists."
}

# Create .venv_dbt if it does not exist
if (-not (Test-Path ".\.venv_dbt")) {
    python -m venv ".\.venv_dbt"
    Write-Host "Created virtual environment: .venv_dbt"
} else {
    Write-Host "Virtual environment '.venv_dbt' already exists."
}

# Activate .venv_airflow and install requirements if present
Write-Host "Activating .venv_airflow and installing requirements..."
& ".\.venv_airflow\Scripts\Activate.ps1"
python -m pip install --upgrade pip
if (Test-Path "requirements.txt") {
    pip install -r requirements.txt
} else {
    Write-Host "requirements.txt not found. Skipping requirements installation."
}

# Activate .venv_dbt and install dbt dependencies
Write-Host "Activating .venv_dbt and installing dbt dependencies..."
& ".\.venv_dbt\Scripts\Activate.ps1"
python -m pip install --upgrade pip
pip install dbt-core dbt-postgres

Clear-Host
# Print completion message
Write-Host "Virtual environments created and activated. Dependencies installed."
Write-Host "You can now use .venv_airflow for Airflow development and .venv_dbt for dbt development."
# Note: To activate the virtual environments in a new terminal, use:
# .\.venv_airflow\Scripts\Activate.ps1
