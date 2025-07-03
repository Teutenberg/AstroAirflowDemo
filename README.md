# Astro Airflow Demo

This project demonstrates a local development environment for Apache Airflow and dbt using Astro Runtime, Docker Compose, and PostgreSQL. It includes:

- Automated setup of Python virtual environments for Airflow and dbt
- Example Airflow DAGs for generating and loading demo banking data
- Pre-configured Postgres database with initialization scripts
- Integration with dbt via Astronomer Cosmos

## Quick Start

1. **Clone the repository**
2. **Install the Astro CLI:**
   - Follow the official instructions for your OS: [Install Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)

3. **Create and activate virtual environments:**
   ```powershell
   . .\activate_venv.ps1
   ```
   - `.venv_airflow` is used for Airflow development (auto-selected in VS Code)
   - `.venv_dbt` is used for dbt CLI (activate manually as needed)

4. **Start the stack:**
   ```sh
   astro dev start
   ```
   This will start Airflow and Postgres, and initialize the demo database using `include/demo_db_create.sql`.

5. **Access Airflow UI:**
   - Navigate to [http://localhost:8080](http://localhost:8080)
   - Default credentials: `admin` / `admin` (unless overridden)

## Project Structure

- `dags/` — Airflow DAGs (see `generate_demo_data.py` for demo data generation)
- `include/demo_db_create.sql` — SQL script to initialize the demo database (used by Docker Compose)
- `requirements.txt` — Python dependencies for Airflow
- `activate_venv.ps1` — Script to set up and activate Python virtual environments
- `airflow_settings.yaml` — Pre-configured Airflow connections for local development
- `docker-compose.override.yml` — Mounts the SQL init script into the Postgres container
- `dbt/` — (optional) Place your dbt project here for dbt development and integration

## Notes

- The Airflow Postgres connection is pre-configured as `pg_demo` (see `airflow_settings.yaml`).
- All demo data is stored as JSONB in the `raw` schema of the `demo` database.
- dbt is installed in a separate venv to avoid dependency conflicts with Airflow.
- Execute dbt commands you must in the dbt project direcotyr: 
    
  `set-location .\dbt\cosmos_demo\`
- Execute astro commands you must in the repo root directory:
    
  `Set-Location (((Get-Location).Path -split 'astro_airflow_demo')[0]+'astro_airflow_demo')`

## Useful Commands

- Activate Airflow venv:  `. .venv_airflow\Scripts\Activate.ps1`
- Activate dbt venv:      `. .venv_dbt\Scripts\Activate.ps1`
- Run dbt CLI:            `dbt --version`

### Astro CLI
- Start local stack:      `astro dev start`
- Stop local stack:       `astro dev stop`
- Restart local stack:    `astro dev restart`
- Kill all containers:    `astro dev kill`
- View logs:              `astro dev logs`
- Open Airflow UI:        `astro dev open` (opens browser to Airflow UI)
- List Airflow DAGs:      `astro dev dags list`
- Export Airflow settings: `astro dev object export`
- Import Airflow settings: `astro dev object import`

---

For more details, see comments in each file or reach out to the project maintainer.
