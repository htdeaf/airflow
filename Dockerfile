FROM quay.io/astronomer/astro-runtime:11.1.0

ENV SODA_PROJECT_DIR=/usr/local/airflow/soda \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# Copier les configs Soda depuis le dossier soda/
COPY soda/ /usr/local/airflow/soda/

# Copier et installer les requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier les DAGs
COPY dags/ /usr/local/airflow/dags/
# COPY include/ /usr/local/airflow/include/
# COPY plugins/ /usr/local/airflow/plugins/