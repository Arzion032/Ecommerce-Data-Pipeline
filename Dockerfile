FROM quay.io/astronomer/astro-runtime:12.6.0

COPY include/.kaggle/kaggle.json /home/astro/.config/kaggle/kaggle.json

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate