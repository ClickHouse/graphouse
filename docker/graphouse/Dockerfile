FROM openjdk:8

ARG USER=graphouse
ARG GRAPHOUSE_ROOT=/opt/graphouse

RUN apt-get update && \
    apt-get install --no-install-recommends --allow-unauthenticated -y apt-transport-https git ca-certificates curl nano net-tools telnet bind9-host && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
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
    
ADD run.py /usr/local/bin/run.py
RUN chmod +x /usr/local/bin/run.py

EXPOSE 2003 2005
USER $USER
WORKDIR $GRAPHOUSE_ROOT

CMD ["python", "-u", "/usr/local/bin/run.py"]
