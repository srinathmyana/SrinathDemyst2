FROM apache/airflow:2.7.2-python3.10

WORKDIR /opt/airflow

COPY process_anonymize_csv.py /opt/airflow/
COPY large_input.csv /opt/airflow/
COPY output /opt/airflow/output

RUN pip install pandas psycopg2-binary

EXPOSE 8080

ENTRYPOINT ["/bin/bash", "-c", "airflow standalone"]
