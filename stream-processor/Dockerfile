FROM flink:1.18.1-java11

USER root

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink/usrlib

COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY app.py .
COPY settings.py .

USER flink

CMD ["bash", "-c", "flink run -py /opt/flink/usrlib/app.py"]