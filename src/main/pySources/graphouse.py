import itertools
import json
import re
import time
import traceback
import urllib

import requests

from django.conf import settings

from graphite.conductor import Conductor
from graphite.logger import log

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

import graphite.readers

conductor = Conductor()

graphouse_url = getattr(settings, 'GRAPHOUSE_URL', 'http://localhost:7000')

def conductor_glob(queries):
    result = set()
    for query in queries:
        parts = query.split('.')
        for (index, part) in enumerate(parts):
            if conductor.CONDUCTOR_EXPR_RE.match(part):
                hosts = conductor.expandExpression(part)
                hosts = [host.replace('.', '_') for host in hosts]

                if len(hosts) > 0:
                    parts[index] = hosts
                else:
                    parts[index] = [part]
            else:
                parts[index] = [part]
        result.update(['.'.join(p) for p in itertools.product(*parts)])
    return list(result)


# Load metrics
class GraphouseFinder(object):

    # Need check. Maybe this method is deprecated
    def prepare_queries(self, pattern):
        queries = self.expand_braces(pattern)
        queries = conductor_glob(queries)

        return queries


    def _expand_braces_part(self, part, braces_re):
        match = braces_re.search(part)
        if not match:
            return [part]

        result = set()

        start_pos, end_pos = match.span(1)
        for item in match.group(1).strip('{}').split(','):
            result.update(self._expand_braces_part(part[:start_pos] + item + part[end_pos:], braces_re))

        return list(result)

    def expand_braces(self, query):
        braces_re = re.compile('({[^{},]*,[^{}]*})')

        parts = query.split('.')
        for (index, part) in enumerate(parts):
            parts[index] = self._expand_braces_part(part, braces_re)

        result = set(['.'.join(p) for p in itertools.product(*parts)])
        return list(result)

    """
    Graphite method
    Find metrics in database and build metrics tree.
    """
    def find_nodes(self, query, req_key):

        result = []
        for query in self.prepare_queries(query.pattern):
            request = requests.get('%s/search?%s' % (graphouse_url, urllib.urlencode({'query': query})))
            request.raise_for_status()

            result += request.text.split('\n')

        for metric in result:
            if not metric:
                continue

            if metric.endswith('.'):
                yield BranchNode(metric[:-1])
            else:
                yield LeafNode(metric, GraphouseReader(metric, req_key))


# Data reader
class GraphouseReader(object):
    __slots__ = ('path', 'nodes', 'reqkey')

    def __init__(self, path, reqkey=''):
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
                    'startSecond': start_time,
                    'endSecond': end_time,
                    'reqKey': self.reqkey
                })
            request_url = graphouse_url + "/metricData"
            request = requests.post(request_url, params=query)

            log.info('DEBUG:graphouse_data_query: %s parameters %s' % (request_url, query))

            request.raise_for_status()
        except Exception as e:
            log.info("Failed to fetch data, got exception:\n %s" % traceback.format_exc())
            return []

        profilingTime['fetch'] = time.time()

        response = json.loads(request.text)
        profilingTime['parse'] = time.time()

        result = [(node, (response.get(node.path).get("timeInfo"), response.get(node.path).get("data"))) for node in self.nodes]
        profilingTime['convert'] = time.time()

        log.info('DEBUG:graphouse_time:[%s] full = %s fetch = %s, parse = %s, convert = %s' % (
            self.reqkey,
            profilingTime['convert'] - profilingTime['start'],
            profilingTime['fetch'] - profilingTime['start'],
            profilingTime['parse'] - profilingTime['fetch'],
            profilingTime['convert'] - profilingTime['parse']
        ))

        if len(result[0]) == 0:
            return []

        if self.path:
            return result[0][1]

        return result


graphite.readers.MultiReader = GraphouseReader