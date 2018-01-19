FROM java:8

ARG repo_ch="deb http://repo.yandex.ru/clickhouse/trusty stable main"
ARG repo_ch_key="E0C56BD4"
ARG ch_version=\*

ARG USER=graphouse
ARG GRAPHOUSE_ROOT=/opt/graphouse

RUN cd /tmp/ && \
    echo $repo_ch | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys $repo_ch_key && \
    apt-get update && \
    apt-get install -y graphouse=$ch_version && \
    apt-get clean
    
ADD run.sh /usr/local/bin/run.sh
RUN chmod +x /usr/local/bin/run.sh

EXPOSE 2003 2005
USER $USER
WORKDIR $GRAPHOUSE_ROOT

CMD ["bash","/usr/local/bin/run.sh"]
