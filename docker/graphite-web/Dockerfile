FROM ubuntu:xenial

RUN apt-get -y update && \
    apt-get -y install python-dev python-pip libcairo2-dev libffi-dev python-requests build-essential && \
    apt-get -y install git nginx uwsgi sqlite3 curl nano net-tools telnet bind9-host psmisc


RUN git clone -b 1.0.2 --depth 1 https://github.com/graphite-project/graphite-web.git /usr/local/src/graphite-web && \
    git clone https://github.com/yandex/graphouse.git /usr/local/src/graphouse

WORKDIR /usr/local/src/graphite-web


RUN sed -i -e 's/whitenoise/whitenoise==3.3.1/g; s/cairocffi/cairocffi==0.9.0/g' requirements.txt  && \
  pip install -r requirements.txt psycopg2 futures tornado gevent eventlet && \
 python ./setup.py install

WORKDIR /opt/graphite/webapp
RUN mkdir -p /var/log/graphite/ && \
    PYTHONPATH=/opt/graphite/webapp django-admin.py collectstatic --noinput --settings=graphite.settings && \
    PYTHONPATH=/opt/graphite/webapp django-admin.py migrate --settings=graphite.settings --run-syncdb

RUN cp /usr/local/src/graphouse/src/main/pySources/graphouse.py /opt/graphite/webapp/graphite/graphouse.py && \
    rm /etc/nginx/sites-enabled/default && \
    cp /opt/graphite/conf/graphite.wsgi.example /opt/graphite/conf/graphite.wsgi
ADD src/local_settings.py /opt/graphite/webapp/graphite/local_settings.py
ADD src/nginx-graphite-web.conf /etc/nginx/sites-enabled/graphite-web.conf
ADD src/uwsgi-graphite-web.ini /etc/uwsgi/apps-enabled/graphite-web.ini

ADD src/run.sh /usr/local/bin/run.sh
RUN chmod +x /usr/local/bin/run.sh

WORKDIR /opt/graphite/webapp

EXPOSE 80

CMD ["bash","/usr/local/bin/run.sh"]
