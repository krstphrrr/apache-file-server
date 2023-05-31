FROM apache/airflow:2.5.2
COPY ./requirements.txt /
USER root
# RUN apt update
# RUN apt install postgresql postgresql-contrib -y
USER airflow

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt