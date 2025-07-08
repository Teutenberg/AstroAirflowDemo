# https://www.astronomer.io/docs/astro/runtime-image-architecture
FROM quay.io/astronomer/astro-runtime:13.1.0

# install dbt into a virtual environment using requirements file
# It’s recommended to use a virtual environment because dbt and Airflow can have conflicting dependencies.
# https://astronomer.github.io/astronomer-cosmos/getting_started/astro.html
COPY requirements.venv_dbt.txt ./
RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r requirements.venv_dbt.txt && \
    deactivate