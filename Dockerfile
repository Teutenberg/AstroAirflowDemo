# https://www.astronomer.io/docs/astro/runtime-image-architecture
FROM quay.io/astronomer/astro-runtime:13.0.0


# install dbt into a virtual environment
# It’s recommended to use a virtual environment because dbt and Airflow can have conflicting dependencies.
# https://astronomer.github.io/astronomer-cosmos/getting_started/astro.html
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres && deactivate