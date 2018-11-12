#!/usr/bin/env bash
set -e

service nginx start
export PYTHONPATH=/opt/graphite/webapp
exec /usr/local/bin/gunicorn wsgi --workers=24 --bind=127.0.0.1:8080 --log-file=/var/log/gunicorn.log --preload --pythonpath=/opt/graphite/webapp/graphite