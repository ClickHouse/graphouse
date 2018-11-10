import ast
import os

LOG_DIR = '/var/log/graphite'
SECRET_KEY = '$(date +%s | sha256sum | base64 | head -c 64)'

if (os.getenv("MEMCACHE_HOSTS") is not None):
    MEMCACHE_HOSTS = [host.strip() for host in os.getenv("MEMCACHE_HOSTS").split(',')]

if (os.getenv("DEFAULT_CACHE_DURATION") is not None):
    DEFAULT_CACHE_DURATION = int(os.getenv("CACHE_DURATION"))

STORAGE_FINDERS = (
    'graphite.graphouse.GraphouseFinder',
)

if (os.getenv("GRAPHOUSE_URL") is not None):
    GRAPHOUSE_URL = os.getenv("GRAPHOUSE_URL")

if (os.getenv("TIME_ZONE") is not None):
    TIME_ZONE = os.getenv("TIME_ZONE")

if (os.getenv("DATABASES") is not None):
    DATABASES = ast.literal_eval(os.getenv("DATABASES"))

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': 1024 * 1024 * 1024,
            'backupCount': 5,
            'filename': '/var/log/graphite/django.log',
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

if (os.path.exists("/opt/graphite/webapp/graphite/additional_local_settings.py")):
    from graphite.additional_local_settings import *
