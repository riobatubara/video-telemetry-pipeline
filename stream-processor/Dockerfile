FROM apache/flink:1.19.1-scala_2.12-java11

USER root

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add connectors
WORKDIR /opt/flink/lib
COPY libs/*.jar .

WORKDIR /opt/flink/usrlib

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY app.py .

USER flink

ENV PYTHONUNBUFFERED=1

CMD ["python3", "-u", "/opt/flink/usrlib/app.py"]