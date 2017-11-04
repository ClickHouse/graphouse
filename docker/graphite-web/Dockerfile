FROM ubuntu:xenial

RUN apt-get -y update && \
    apt-get -y install python-dev python-pip libcairo2-dev libffi-dev python-requests build-essential && \
    apt-get -y install git nginx uwsgi curl sqlite3 nano


RUN git clone -b 1.0.2 --depth 1 https://github.com/graphite-project/graphite-web.git /usr/local/src/graphite-web && \
    git clone https://github.com/yandex/graphouse.git /usr/local/src/graphouse

WORKDIR /usr/local/src/graphite-web

RUN pip install -r requirements.txt && \
 python ./setup.py install

WORKDIR /opt/graphite/webapp
RUN mkdir -p /var/log/graphite/ && \
    PYTHONPATH=/opt/graphite/webapp django-admin.py collectstatic --noinput --settings=graphite.settings && \
    PYTHONPATH=/opt/graphite/webapp django-admin.py migrate --settings=graphite.settings --run-syncdb

RUN cp /usr/local/src/graphouse/src/main/pySources/graphouse.py /opt/graphite/webapp/graphite/graphouse.py && \
    rm /etc/nginx/sites-enabled/default && \
    cp /opt/graphite/conf/graphite.wsgi.example /opt/graphite/conf/graphite.wsgi
ADD conf/local_settings.py /opt/graphite/webapp/graphite/local_settings.py
ADD conf/nginx-graphite-web.conf /etc/nginx/sites-enabled/graphite-web.conf
ADD conf/uwsgi-graphite-web.ini /etc/uwsgi/apps-enabled/graphite-web.ini

WORKDIR /opt/graphite/webapp

EXPOSE 80

CMD service nginx start && \
    export PYTHONPATH=/opt/graphite/webapp && \
    exec /usr/local/bin/gunicorn wsgi --workers=4 --bind=127.0.0.1:8080 --log-file=/var/log/gunicorn.log --preload --pythonpath=/opt/graphite/webapp/graphite
