FROM debian:jessie
ENV DEBIAN_FRONTEND noninteractive

ARG repo_java="deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main"
ARG repo_java_src="deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main"
ARG repo_java_key="EEA14886"
ARG USER=graphouse
ARG GRAPHOUSE_ROOT=/opt/graphouse


RUN apt-get update && \
    apt-get install -y --no-install-recommends apt-transport-https git ca-certificates && \
    mkdir -p /etc/apt/sources.list.d && \
    echo $repo_java | tee /etc/apt/sources.list.d/java.list && \
    echo $repo_java_src | tee -a /etc/apt/sources.list.d/java.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys $repo_java_key && \
    apt-get update && \
    echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
    echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections && \
    apt-get install --no-install-recommends --allow-unauthenticated -y oracle-java8-installer && \
    apt-get install --no-install-recommends --allow-unauthenticated -y oracle-java8-set-default && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf /var/cache/oracle-* && \
    apt-get clean

RUN cd /tmp/ && \
    git clone https://github.com/yandex/graphouse.git && \
    cd graphouse && \
    JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8 ./gradlew build installDist && \
    mv /tmp/graphouse/build/install/graphouse /opt/ && \
    useradd ${USER} -g nogroup 2>/dev/null && \
    chown -R $USER:nogroup $GRAPHOUSE_ROOT && \
    ln -sf $GRAPHOUSE_ROOT/conf/ /etc/graphouse && \
    ln -sf $GRAPHOUSE_ROOT/log/ /var/log/graphouse && \
    rm -rf /tmp/graphouse
    
ADD run.sh /usr/local/bin/run.sh
RUN chmod +x /usr/local/bin/run.sh

EXPOSE 2003 2005
USER $USER
WORKDIR $GRAPHOUSE_ROOT

CMD ["bash","/usr/local/bin/run.sh"]
