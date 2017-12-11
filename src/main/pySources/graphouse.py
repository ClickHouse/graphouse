import time
import traceback
import requests
import graphite.readers
import uuid

from django.conf import settings
from graphite.intervals import IntervalSet, Interval
from graphite.logger import log
from graphite.node import LeafNode, BranchNode

graphouse_url = getattr(settings, 'GRAPHOUSE_URL', 'http://localhost:2005')


class GraphouseMultiFetcher(object):
    def __init__(self):
        self.nodes = []
        self.result = {}

    def append(self, node):
        self.nodes.append(node)

    def fetch(self, path, reqkey, start_time, end_time):
        if path in self.result:
            return self.result[path]

        if reqkey is None:
            reqkey = str(uuid.uuid4())

        profilingTime = {'start': time.time()}

        try:
            paths = [node.path.replace('\'', '\\\'') for node in self.nodes]

            query = {
                        'start': start_time,
                        'end': end_time,
                        'reqKey': reqkey
                    }
            data = {'metrics': ','.join(paths)}
            request_url = graphouse_url + "/metricData"
            request = requests.post(request_url, params=query, data=data)

            log.info('DEBUG:graphouse_data_query: %s parameters %s, data %s', request_url, query, data)

            request.raise_for_status()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            log.info("CRITICAL:graphouse_data_query: Connection error: %s", str(e))
            raise
        except requests.exceptions.HTTPError as e:
            log.info("CRITICAL:graphouse_data_query: %s, message: %s", str(e), request.text)
            raise
        except Exception:
            log.info("Unexpected exception!", exc_info=True)
            raise

        profilingTime['fetch'] = time.time()

        try:
            metrics_object = request.json()
        except Exception:
            log.info("CRITICAL:graphouse_parse: can't parse json from graphouse answer, got '%s'", request.text)
            raise

        profilingTime['parse'] = time.time()

        time_infos = []
        points = []

        for node in self.nodes:
            metric_object = metrics_object.get(node.path)
            if metric_object is None:
                self.result[node.path] = ((start_time, end_time, 1), [])
            else:
                self.result[node.path] = (
                        (metric_object.get("start"), metric_object.get("end"), metric_object.get("step")),
                        metric_object.get("points"),
                    )

        profilingTime['convert'] = time.time()

        log.info('DEBUG:graphouse_time:[%s] full = %s fetch = %s, parse = %s, convert = %s',
            reqkey,
            profilingTime['convert'] - profilingTime['start'],
            profilingTime['fetch'] - profilingTime['start'],
            profilingTime['parse'] - profilingTime['fetch'],
            profilingTime['convert'] - profilingTime['parse']
        )

        result = self.result.get(path)
        if result is None:
            log.info("WARNING:graphouse_data: something strange: path %s doesn't exist in graphouse response", path)
            raise ValueError("path %s doesn't exist in graphouse response" % path)
        else:
            return result



class GraphouseFinder(object):

    def find_nodes(self, query):
        request = requests.get('%s/search' % graphouse_url, params={'query': query.pattern})
        request.raise_for_status()
        result = request.text.split('\n')

        fetcher = GraphouseMultiFetcher()
        for metric in result:
            if not metric:
                continue
            if metric.endswith('.'):
                yield BranchNode(metric[:-1])
            else:
                yield LeafNode(metric, GraphouseReader(metric, fetcher=fetcher))


# Data reader
class GraphouseReader(object):
    __slots__ = ('path', 'nodes', 'reqkey', 'fetcher')

    def __init__(self, path, reqkey=None, fetcher=None):
        self.nodes = [self]
        self.path = None

        if fetcher is not None:
            fetcher.append(self)

        self.fetcher = fetcher

        if hasattr(path, '__iter__'):
            self.nodes = path
        else:
            self.path = path

        self.reqkey = reqkey

    """
    Graphite method
    Hints graphite-web about the time range available for this given metric in the database
    """

    def get_intervals(self):
        return IntervalSet([Interval(0, int(time.time()))])

    """
    Graphite method
    :return list of 2 elements (time_info, data_points)
    time_info - list of 3 elements (start_time, end_time, time_step)
    data_points - list of ((end_time - start_time) / time_step) points, loaded from database
    """

    def fetch(self, start_time, end_time):
        return self.fetcher.fetch(self.path, self.reqkey, start_time, end_time)


graphite.readers.MultiReader = GraphouseReader
