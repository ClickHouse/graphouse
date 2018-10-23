FROM java:8

ARG USER=graphouse
ARG GRAPHOUSE_ROOT=/opt/graphouse
ARG repo="deb http://repo.yandex.ru/graphouse/trusty stable main"
ARG repo_key="E0C56BD4"
ARG gh_version="1.10"

RUN echo $repo | tee /etc/apt/sources.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys $repo_key && \
    apt-get update && \
    apt-get install -y graphouse=$gh_version && \
    apt-get clean
    
ADD run.sh /usr/local/bin/run.sh
RUN chmod +x /usr/local/bin/run.sh

EXPOSE 2003 2005
USER $USER
WORKDIR $GRAPHOUSE_ROOT

CMD ["bash","/usr/local/bin/run.sh"]
