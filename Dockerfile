FROM python:3.9.1

WORKDIR /usr/src/app/api

RUN apt-get update && apt-get clean

RUN apt-get -y install supervisor && \
  mkdir -p /var/log/supervisor && \
  mkdir -p /etc/supervisor/conf.d

COPY requirements.txt ./requirements.txt
RUN pip install urllib3
RUN pip install -r requirements.txt

RUN apt update -y && apt-get install -y software-properties-common && \
    apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' && apt update -y && \
    apt-get install -y openjdk-8-jdk-headless && \
    export JAVA_HOME && \
    apt-get clean

COPY analytics_results ./analytics_results
COPY app ./app
COPY assets ./assets
COPY checkpoints ./checkpoints

COPY app_main.py ./app_main.py

COPY startup.sh ./startup.sh
RUN chmod 777 ./startup.sh && \
    sed -i 's/\r//' ./startup.sh

COPY VERSION ./VERSION

RUN mkdir ./logs
RUN chown 1000 ./logs && chgrp 1000 ./logs && chmod 777 ./logs
EXPOSE 5000

CMD ["./startup.sh"]
