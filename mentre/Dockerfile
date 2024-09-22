FROM apache/airflow:2.10.1

USER root
RUN apt-get update && \
    apt-get install -y gcc libssl-dev libncurses-dev

USER airflow
RUN pip install --upgrade pip
RUN pip install wheel

COPY requirements.txt .
RUN pip install -r requirements.txt

ARG PROJECT_ROOT=/opt/airflow/dags
ENV PYTHONPATH=${PROJECT_ROOT}
WORKDIR ${PROJECT_ROOT}
