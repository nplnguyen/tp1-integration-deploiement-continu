FROM apache/spark:3.5.7

USER root

RUN pip install --no-cache-dir psycopg2-binary==2.9.9

COPY spark/sales_stream.py /opt/spark-app/sales_stream.py

WORKDIR /opt/spark-app