import time
import traceback
import requests

from structlog import get_logger
from graphite_api.intervals import IntervalSet, Interval
from graphite_api.node import LeafNode, BranchNode

logger = get_logger()

class GraphouseFinder(object):
    def __init__(self, config):
        config.setdefault('graphouse', {})
        self.graphouse_url = config['graphouse'].get('url', 'http://localhost:2005')

    def find_nodes(self, query):
        request = requests.post('%s/search' % self.graphouse_url, data={'query': query.pattern})
        request.raise_for_status()
        result = request.text.split('\n')

        for metric in result:
            if not metric:
                continue
            if metric.endswith('.'):
                yield BranchNode(metric[:-1])
            else:
                yield LeafNode(metric, GraphouseReader(metric, graphouse_url=self.graphouse_url))


# Data reader
class GraphouseReader(object):
    __slots__ = ('path', 'nodes', 'reqkey', 'graphouse_url')

    def __init__(self, path, reqkey='empty', graphouse_url='http://localhost:2005'):
        self.nodes = [self]
        self.path = None
        self.graphouse_url = graphouse_url

        if hasattr(path, '__iter__') and type(path) != str:
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

    def fetch(self, start_time, end_time, *arg):
        profilingTime = {'start': time.time()}

        try:
            paths = [node.path.replace('\'', '\\\'') for node in self.nodes]

            query = {
                        'start': start_time,
                        'end': end_time,
                        'reqKey': self.reqkey
                    }
            data = {'metrics': ','.join(paths)}
            request_url = self.graphouse_url + "/metricData"
            request = requests.post(request_url, params=query, data=data)

            logger.info('DEBUG:graphouse_data_query: %s parameters %s' % (request_url, query))

            request.raise_for_status()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            logger.info("CRITICAL:graphouse_data_query: Connection error: %s", str(e))
            raise
        except requests.exceptions.HTTPError as e:
            logger.info("CRITICAL:graphouse_data_query: %s, message: %s", str(e), request.text)
            raise
        except Exception:
            logger.info("Unexpected exception!", exc_info=True)
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
                time_infos += (0, 0, 1)
                points += []
            else:
                time_infos += (metric_object.get("start"), metric_object.get("end"), metric_object.get("step"))
                points += metric_object.get("points")

        profilingTime['convert'] = time.time()

        logger.info('DEBUG:graphouse_time:[%s] full = %s fetch = %s, parse = %s, convert = %s' % (
            self.reqkey,
            profilingTime['convert'] - profilingTime['start'],
            profilingTime['fetch'] - profilingTime['start'],
            profilingTime['parse'] - profilingTime['fetch'],
            profilingTime['convert'] - profilingTime['parse']
        ))

        return time_infos, points
