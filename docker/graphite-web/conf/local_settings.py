import os
import ast

LOG_DIR = '/var/log/graphite'
SECRET_KEY = '$(date +%s | sha256sum | base64 | head -c 64)'

if (os.getenv("MEMCACHE_HOST") is not None):
    MEMCACHE_HOSTS = os.getenv("MEMCACHE_HOST").split(",")

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

