import time
import traceback  # noqa: F401
import requests
import uuid

from django.conf import settings
from graphite.intervals import IntervalSet, Interval
from graphite.logger import log
from graphite.node import LeafNode, BranchNode
try:
    from graphite.worker_pool.pool import (
        Job, get_pool, pool_exec as worker_pool_exec
    )
    from graphite.finders.utils import BaseFinder
    from graphite.storage import Store
except ImportError:
    # Backward compatibility with older than 1.1
    class BaseFinder(object):
        def __init__(self):
            pass

    class Store(object):
        pass

    log.debug = log.warning = log.info


graphouse_url = getattr(settings, 'GRAPHOUSE_URL', 'http://localhost:2005')
# Amount of threads used to fetch data from graphouse
parallel_jobs = getattr(settings, 'GRAPHOUSE_PARALLEL_JOBS', 2)
# See graphouse.http.max-form-context-size-bytes property
max_data_size = getattr(settings, 'GRAPHOUSE_MAX_FORM_SIZE', 500000)
GRAPHITE_VERSION = tuple(
    int(i) for i in getattr(settings, 'WEBAPP_VERSION').split('.')
)


class GraphouseMultiFetcher(object):
    def __init__(self):
        self.nodes = []
        self.result = {}
        self.paths = []
        self.body_size = len('metrics')

    def append(self, node):
        path = node.path.replace("'", "\\'")
        # additional url encoded coma or equal sign
        node_size = len(requests.utils.quote(path)) + 3
        if (self.body_size + node_size) <= max_data_size:
            self.nodes.append(node)
            self.body_size += node_size
            self.paths.append(path)
        else:
            raise OverflowError(
                'Maximum body size {} exceeded'.format(max_data_size)
            )

    def fetch(self, path, reqkey, start_time, end_time, requestContext=None):
        if path in self.result:
            return self.result[path]

        reqkey = reqkey or str(uuid.uuid4())

        requestContext = requestContext or {}
        maxDataPoints = requestContext.get('maxDataPoints')

        profilingTime = {'start': time.time()}

        try:
            query = {
                'start': start_time,
                'end': end_time,
                'reqKey': reqkey,
            }
            if maxDataPoints:
                query['maxDataPoints'] = maxDataPoints

            data = {'metrics': ','.join(self.paths)}
            request_url = graphouse_url + "/metricData"
            request = requests.post(request_url, params=query, data=data)

            log.debug(
                'DEBUG:graphouse_data_query: {} parameters {}, data {}'
                .format(request_url, query, data)
            )

            request.raise_for_status()
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout
        ) as e:
            log.warning('CRITICAL:graphouse_data_query: Connection error: {}'
                        .format(str(e)))
            raise
        except requests.exceptions.HTTPError as e:
            log.warning('CRITICAL:graphouse_data_query: {}, message: {}'
                        .format(str(e), request.text))
            raise
        except Exception:
            log.warning('Unexpected exception!', exc_info=True)
            raise

        profilingTime['fetch'] = time.time()

        try:
            metrics_object = request.json()
        except Exception:
            log.warning(
                'CRITICAL:graphouse_parse: '
                "can't parse json from graphouse answer, got '{}'"
                .format(request.text)
            )
            raise

        profilingTime['parse'] = time.time()

        for node in self.nodes:
            metric_object = metrics_object.get(node.path)
            if metric_object is None:
                self.result[node.path] = ((start_time, end_time, 1), [])
            else:
                self.result[node.path] = (
                    (
                        metric_object.get("start"),
                        metric_object.get("end"),
                        metric_object.get("step")
                    ),
                    metric_object.get("points"),
                )

        profilingTime['convert'] = time.time()

        log.debug(
            'DEBUG:graphouse_time:[{}] full = {} '
            'fetch = {}, parse = {}, convert = {}'.format(
                reqkey,
                profilingTime['convert'] - profilingTime['start'],
                profilingTime['fetch'] - profilingTime['start'],
                profilingTime['parse'] - profilingTime['fetch'],
                profilingTime['convert'] - profilingTime['parse']
            )
        )

        result = self.result.get(path)
        if result is None:
            log.warning(
                'WARNING:graphouse_data: something strange: '
                "path {} doesn't exist in graphouse response".format(path)
            )
            raise ValueError(
                "path {} doesn't exist in graphouse response".format(path)
            )
        else:
            return result


class GraphouseFinder(BaseFinder, Store):
    def __init__(self):
        '''
        Explicit execution of BaseFinder.__init__ only
        '''
        BaseFinder.__init__(self)

    def _search_request(self, pattern):
        request = requests.post(
            url='{}/search'.format(graphouse_url),
            data={'query': pattern}
        )
        request.raise_for_status()
        result = (pattern, request.text.split('\n'))
        return result

    def pool_exec(self, jobs, timeout):
        '''
        Overwrite of pool_exec from Store to get another workers pool
        '''
        if not jobs:
            return []

        thread_count = 0
        if settings.USE_WORKER_POOL:
            thread_count = min(parallel_jobs, settings.POOL_MAX_WORKERS)

        return worker_pool_exec(
            get_pool('graphouse', thread_count), jobs, timeout
        )

    def find_nodes(self, query):
        '''
        This method processed in graphite 1.0 and older
        '''
        result = self._search_request(query.pattern)

        fetcher = GraphouseMultiFetcher()
        for metric in result[1]:
            if not metric:
                continue
            if metric.endswith('.'):
                yield BranchNode(metric[:-1])
            else:
                try:
                    yield LeafNode(
                        metric,
                        GraphouseReader(metric, fetcher=fetcher)
                    )
                except OverflowError:
                    fetcher = GraphouseMultiFetcher()
                    yield LeafNode(
                        metric,
                        GraphouseReader(metric, fetcher=fetcher)
                    )

    def find_multi(self, patterns, reqkey=None):
        '''
        This method processed in graphite 1.1 and newer from self.fetch
        Returns:
            Generator of (pattern, [nodes])
        '''
        reqkey = reqkey or uuid.uuid4()
        jobs = [
            Job(self._search_request,
                'Query graphouse for {}'.format(pattern), pattern)
            for pattern in patterns
        ]

        results = self.wait_jobs(
            jobs, getattr(settings, 'FIND_TIMEOUT'),
            'Find nodes for {} request'.format(reqkey)
        )

        for pattern, metric_names in results:
            leafs = []
            for metric in metric_names:
                if metric and not metric.endswith('.'):
                    leafs.append(LeafNode(metric, None))
            yield (pattern, leafs)

    def fetch(self, patterns, start_time, end_time, now=None,
              requestContext=None):
        """Fetch multiple patterns at once.

        This method is used to fetch multiple patterns at once, this
        allows alternate finders to do batching on their side when they can.

        Returns:
          an iterable of
          {
            'pathExpression': pattern,
            'path': node.path,
            'name': node.path,
            'time_info': time_info,
            'values': values,
          }
        """

        log.debug(
            'Multifetcher, patterns={}, requestContext={}'
            .format(patterns, requestContext)
        )
        profilingTime = {'start': time.time()}

        requestContext = requestContext or {}
        reqkey = uuid.uuid4()
        results = []

        find_results = self.find_multi(patterns, reqkey)
        profilingTime['find'] = time.time()

        current_fetcher = GraphouseMultiFetcher()
        subreqs = [current_fetcher]

        for pair in find_results:
            pattern = pair[0]
            nodes = pair[1]
            log.debug('Results from find_multy={}'.format(pattern, nodes))
            for node in nodes:
                try:
                    current_fetcher.append(node)
                except OverflowError:
                    current_fetcher = GraphouseMultiFetcher()
                    subreqs.append(current_fetcher)
                    current_fetcher.append(node)
                results.append(
                    {
                        'pathExpression': pattern,
                        'name': node.path,
                        'path': node.path,
                        'fetcher': current_fetcher,
                    }
                )

        jobs = [
            Job(
                sub.fetch,
                'Fetch values for reqkey={} for {} metrics'.format(
                    reqkey, len(sub.nodes)
                ),
                sub.nodes[0].path, reqkey, start_time, end_time, requestContext
            ) for sub in subreqs if sub.nodes
        ]
        profilingTime['gen_fetch'] = time.time()

        # Fetch everything in parallel
        _ = self.wait_jobs(
            jobs,
            getattr(settings, 'FETCH_TIMEOUT'),
            'Multifetch for request key {}'.format(reqkey)
        )
        profilingTime['fetch'] = time.time()

        for result in results:
            result['time_info'], result['values'] = result['fetcher'].fetch(
                result['path'], reqkey, start_time, end_time
            )
        profilingTime['fill'] = time.time()

        log.debug(
            'DEBUG:graphouse_multifetch[{}]: full={}, find={}, '
            'generetion_fetch_jobs={}, fetch_graphouse_parallel={}, '
            'parse_values={}'
            .format(
                reqkey,
                profilingTime['fill'] - profilingTime['start'],
                profilingTime['find'] - profilingTime['start'],
                profilingTime['gen_fetch'] - profilingTime['find'],
                profilingTime['fetch'] - profilingTime['gen_fetch'],
                profilingTime['fill'] - profilingTime['fetch'],
            )
        )

        return results


# Data reader for graphite 1.0 and older
class GraphouseReader(object):
    __slots__ = ('path', 'reqkey', 'fetcher')

    def __init__(self, path, reqkey=None, fetcher=None):
        self.path = path

        self.fetcher = fetcher

        if fetcher is not None:
            fetcher.append(self)

        self.reqkey = reqkey

    """
    Graphite method
    Hints graphite-web about the time range available
        for this given metric in the database
    """

    def get_intervals(self):
        return IntervalSet([Interval(0, int(time.time()))])

    """
    Graphite method
    :return list of 2 elements (time_info, data_points)
    time_info - list of 3 elements (start_time, end_time, time_step)
    data_points - list of ((end_time - start_time) / time_step) points,
        loaded from database
    """

    def fetch(self, start_time, end_time):
        return self.fetcher.fetch(self.path, self.reqkey, start_time, end_time)


if GRAPHITE_VERSION[:2] <= (1, 0):
    import graphite.readers
    graphite.readers.MultiReader = GraphouseReader
