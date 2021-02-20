import ast
import os

LOG_DIR = '/var/log/graphite'
SECRET_KEY = '$(date +%s | sha256sum | base64 | head -c 64)'

if os.getenv("MEMCACHE_HOSTS") is not None:
    MEMCACHE_HOSTS = [
        host.strip() for host in os.getenv("MEMCACHE_HOSTS").split(',')
    ]

    # e.g. 'MEMCACHE_HOSTS' env var should be like this value =>   "hostname_1:port_1,...,hostname_n:port_n", n >= 1
    if os.getenv("RESOLVE_MEMCACHE_HOSTS_TO_IPV6") is not None:
        import socket

        resolved_hosts = []
        for mh in MEMCACHE_HOSTS:
            (host, port) = mh.split(':')
            (hostName, _, ip) = socket.gethostbyaddr(host)
            resolved_hosts.append(("inet6:[%s]:%s" % (ip[0], port)))

        MEMCACHE_HOSTS = resolved_hosts

if os.getenv("DEFAULT_CACHE_DURATION") is not None:
    DEFAULT_CACHE_DURATION = int(os.getenv("CACHE_DURATION"))

STORAGE_FINDERS = (
    'graphite.graphouse.GraphouseFinder',
)

#see details in graphouse.py
if os.getenv("GRAPHOUSE_URL") is not None:
    GRAPHOUSE_URL = os.getenv("GRAPHOUSE_URL")

#metric size in bytes to send to graphouse to fetch data
if os.getenv("GRAPHOUSE_MAX_FORM_SIZE") is not None:
    GRAPHOUSE_MAX_FORM_SIZE = int(os.getenv("GRAPHOUSE_MAX_FORM_SIZE"))

if os.getenv("USE_WORKER_POOL") is not None:
    USE_WORKER_POOL = os.getenv("USE_WORKER_POOL") == 'True'

if os.getenv("POOL_MAX_WORKERS") is not None:
    POOL_MAX_WORKERS = int(os.getenv("POOL_MAX_WORKERS"))

if os.getenv("GRAPHOUSE_PARALLEL_JOBS_TO_USE_FAST_POOL") is not None:
    GRAPHOUSE_PARALLEL_JOBS_TO_USE_FAST_POOL = int(os.getenv("GRAPHOUSE_PARALLEL_JOBS_TO_USE_FAST_POOL"))

if os.getenv("GRAPHOUSE_PARALLEL_JOBS_TO_USE_SLOW_POOL") is not None:
    GRAPHOUSE_PARALLEL_JOBS_TO_USE_SLOW_POOL = int(os.getenv("GRAPHOUSE_PARALLEL_JOBS_TO_USE_SLOW_POOL"))

if os.getenv("GRAPHOUSE_NODES_COUNT_TO_USE_SLOW_POOL") is not None:
    GRAPHOUSE_NODES_COUNT_TO_USE_SLOW_POOL = int(os.getenv("GRAPHOUSE_NODES_COUNT_TO_USE_SLOW_POOL"))

if os.getenv("GRAPHOUSE_MAX_NODES_COUNT") is not None:
    GRAPHOUSE_MAX_NODES_COUNT = int(os.getenv("GRAPHOUSE_MAX_NODES_COUNT"))

# by default: EXCEPTION, another policy is CUT_BY_LIMIT
if os.getenv("GRAPHOUSE_NODES_LIMIT_EXCEEDED_POLICY") is not None:
    GRAPHOUSE_NODES_LIMIT_EXCEEDED_POLICY = os.getenv("GRAPHOUSE_NODES_LIMIT_EXCEEDED_POLICY")

if os.getenv("FIND_TIMEOUT") is not None:
    FIND_TIMEOUT = int(os.getenv("FIND_TIMEOUT"))

if os.getenv("FETCH_TIMEOUT") is not None:
    FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT"))


if os.getenv("TIME_ZONE") is not None:
    TIME_ZONE = os.getenv("TIME_ZONE")

if os.getenv("DATABASES") is not None:
    DATABASES = ast.literal_eval(os.getenv("DATABASES"))

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(asctime)s %(message)s'
        },
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': 1024 * 1024 * 1024,
            'backupCount': 5,
            'filename': '/var/log/graphite/django.log',
            'formatter': 'simple',
        },
    },
    'loggers': {
        '': {
            'handlers': ['file'],
            'level': 'INFO',
            'propagate': True,
        },
    },
}

if (os.path.exists(
    "/opt/graphite/webapp/graphite/additional_local_settings.py"
)):
    from graphite.additional_local_settings import *  # noqa: F403, F401
