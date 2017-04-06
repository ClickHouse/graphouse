import json
import time
import traceback
import urllib
import requests
import graphite.readers

from django.conf import settings
from graphite.intervals import IntervalSet, Interval
from graphite.logger import log
from graphite.node import LeafNode, BranchNode

graphouse_url = getattr(settings, 'GRAPHOUSE_URL', 'http://localhost:2005')


class GraphouseFinder(object):

    def find_nodes(self, query):
        request = requests.get('%s/search?%s' % (graphouse_url, urllib.urlencode({'query': query.pattern})))
        request.raise_for_status()
        result = request.text.split('\n')

        for metric in result:
            if not metric:
                continue
            if metric.endswith('.'):
                yield BranchNode(metric[:-1])
            else:
                yield LeafNode(metric, GraphouseReader(metric))


# Data reader
class GraphouseReader(object):
    __slots__ = ('path', 'nodes', 'reqkey')

    def __init__(self, path, reqkey='empty'):
        self.nodes = [self]
        self.path = None

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
        profilingTime = {'start': time.time()}

        try:
            paths = [node.path.replace('\'', '\\\'') for node in self.nodes]

            query = urllib.urlencode(
                {
                    'metrics': ','.join(paths),
                    'start': start_time,
                    'end': end_time,
                    'reqKey': self.reqkey
                })
            request_url = graphouse_url + "/metricData"
            request = requests.post(request_url, params=query)

            log.info('DEBUG:graphouse_data_query: %s parameters %s' % (request_url, query))

            request.raise_for_status()
        except Exception as e:
            log.info("Failed to fetch data, got exception:\n %s" % traceback.format_exc())
            raise e

        profilingTime['fetch'] = time.time()

        metrics_object = json.loads(request.text)
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

        log.info('DEBUG:graphouse_time:[%s] full = %s fetch = %s, parse = %s, convert = %s' % (
            self.reqkey,
            profilingTime['convert'] - profilingTime['start'],
            profilingTime['fetch'] - profilingTime['start'],
            profilingTime['parse'] - profilingTime['fetch'],
            profilingTime['convert'] - profilingTime['parse']
        ))

        return time_infos, points

graphite.readers.MultiReader = GraphouseReader
