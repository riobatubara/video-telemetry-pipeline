FROM flink:1.18.1-scala_2.12-java17

WORKDIR /opt/flink

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*
USER flink

COPY requirements.txt /opt/flink/requirements.txt

RUN pip install --no-cache-dir -r /opt/flink/requirements.txt

COPY libs/ /opt/flink/lib/

COPY job/ /opt/flink/job/
COPY sinks/ /opt/flink/sinks/
COPY main.py /opt/flink/main.py

ENV PYTHONPATH=/opt/flink

CMD ["python3", "/opt/flink/main.py"]