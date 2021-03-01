import time
import traceback  # noqa: F401
import uuid

import requests
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
parallel_jobs_for_fast_pool = getattr(settings, 'GRAPHOUSE_PARALLEL_JOBS_TO_USE_FAST_POOL', 10)
parallel_jobs_for_slow_pool = getattr(settings, 'GRAPHOUSE_PARALLEL_JOBS_TO_USE_SLOW_POOL', 2)
# See graphouse.http.max-form-context-size-bytes property
max_data_size = getattr(settings, 'GRAPHOUSE_MAX_FORM_SIZE', 500000)
# the number of nodes should be set to limit memory consumption.
# Nodes mean how many metrics will be unfolded by wildcards
max_allowed_nodes_count = getattr(settings, 'GRAPHOUSE_MAX_NODES_COUNT', 25000)
# Big requests (with many nodes / wildcards) might cause stuck all other requests,
# so it's a reason to put them to another pool.
nodes_count_to_use_slow_pool = getattr(settings, 'GRAPHOUSE_NODES_COUNT_TO_USE_SLOW_POOL', 5000)
find_timeout_seconds = getattr(settings, 'FIND_TIMEOUT', 60)
nodes_limit_exceeded_policy = getattr(settings, 'GRAPHOUSE_NODES_LIMIT_EXCEEDED_POLICY', 'EXCEPTION')
fetch_timeout_seconds = getattr(settings, 'FETCH_TIMEOUT', 600)

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

    def fetch(self, path, req_key, start_time, end_time):
        if path in self.result:
            return self.result[path]

        if req_key is None:
            req_key = str(uuid.uuid4())

        profiling_time = {'start': time.time()}

        try:
            query = {
                'start': start_time,
                'end': end_time,
                'reqKey': req_key
            }
            data = {'metrics': ','.join(self.paths)}
            request_url = graphouse_url + "/metricData"
            request = requests.post(request_url, params=query, data=data)

            log.debug('DEBUG:graphouse_data_query: {} parameters {}, data {}'.format(request_url, query, data))

            request.raise_for_status()
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout
        ) as e:
            log.warning('CRITICAL:graphouse_data_query: Connection error: {}'.format(str(e)))
            raise
        except requests.exceptions.HTTPError as e:
            log.warning('CRITICAL:graphouse_data_query: {}, message: {}'.format(str(e), request.text))
            raise
        except Exception:
            log.warning('Unexpected exception!', exc_info=True)
            raise

        profiling_time['fetch'] = time.time()

        try:
            metrics_object = request.json()
        except Exception:
            log.warning(
                'CRITICAL:graphouse_parse: '
                "can't parse json from graphouse answer, got '{}'".format(request.text)
            )
            raise

        profiling_time['parse'] = time.time()

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

        profiling_time['convert'] = time.time()

        log.debug(
            'DEBUG:graphouse_time:[{}] full = {} '
            'fetch = {}, parse = {}, convert = {}'.format(
                req_key,
                profiling_time['convert'] - profiling_time['start'],
                profiling_time['fetch'] - profiling_time['start'],
                profiling_time['parse'] - profiling_time['fetch'],
                profiling_time['convert'] - profiling_time['parse']
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
        self.pool_name = 'graphouse'
        self.thread_count = 10

    def _search_request(self, pattern):
        request = requests.post(
            url='{}/search'.format(graphouse_url),
            data={'query': pattern}
        )
        request.raise_for_status()
        result = (pattern, request.text.split('\n'))
        return result

    def prepare_fast_pool(self, req_key):
        self.pool_name = 'graphouse_fast_requests_pool'

        if settings.USE_WORKER_POOL:
            self.thread_count = min(parallel_jobs_for_fast_pool, settings.POOL_MAX_WORKERS)
        log.debug('DEBUG[{}]: Using fast pool with "{}" threads'.format(req_key, self.thread_count))

    def prepare_slow_pool(self, req_key):
        self.pool_name = 'graphouse_slow_requests_pool'

        if settings.USE_WORKER_POOL:
            self.thread_count = min(parallel_jobs_for_slow_pool, settings.POOL_MAX_WORKERS)
        log.debug('DEBUG[{}]: Using slow pool with "{}" threads'.format(req_key, self.thread_count))

    def pool_exec(self, jobs, timeout):
        '''
        Overwrite of pool_exec from Store to get another workers pool
        '''
        if not jobs:
            return []

        return worker_pool_exec(
            get_pool(self.pool_name, self.thread_count), jobs, timeout
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

    def find_multi(self, patterns, req_key=None):
        '''
        This method processed in graphite 1.1 and newer from self.fetch
        Returns:
            Generator of (pattern, [nodes])
        '''
        req_key = req_key or uuid.uuid4()
        jobs = [
            Job(self._search_request,
                'Query graphouse for {}'.format(pattern), pattern)
            for pattern in patterns
        ]

        results = self.wait_jobs(
            jobs, find_timeout_seconds,
            'Find nodes for {} request'.format(req_key)
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

        req_key = uuid.uuid4()
        log.debug('reqKey:{},  Multifetcher, patternCount: {},  patterns={}'.format(req_key, len(patterns), patterns))

        profiling_time = {'start': time.time()}
        requestContext = requestContext or {}
        context_to_log = requestContext.copy()
        context_to_log['prefetched'] = {}
        log.debug('reqKey:{},  requestContext:{}'.format(req_key, context_to_log))

        results = []

        find_results = self.find_multi(patterns, req_key)
        profiling_time['find'] = time.time()

        current_fetcher = GraphouseMultiFetcher()
        sub_reqs = [current_fetcher]
        find_results_count = 0
        total_nodes_count = 0
        processed_nodes_count = 0

        for pair in find_results:
            find_results_count += 1
            pattern = pair[0]
            nodes = pair[1]
            total_nodes_count += len(nodes)
            log.debug(
                'reqKey:{} Results from find_multi, pattern:{}, nodesCount:{}'.format(req_key, pattern, len(nodes))
            )

            for node in nodes:
                if processed_nodes_count >= max_allowed_nodes_count:
                    if nodes_limit_exceeded_policy == 'EXCEPTION':
                        raise Exception(
                            'Max nodes (wildcards / substitutions) "{}" exceeded by patterns:{}'.format(
                                max_allowed_nodes_count, patterns
                            )
                        )
                    else:  # CUT_BY_LIMIT
                        break

                try:
                    current_fetcher.append(node)
                except OverflowError:
                    current_fetcher = GraphouseMultiFetcher()
                    sub_reqs.append(current_fetcher)
                    log.debug('reqKey:{},  subreq size:{}'.format(req_key, len(sub_reqs)))
                    current_fetcher.append(node)
                results.append(
                    {
                        'pathExpression': pattern,
                        'name': node.path,
                        'path': node.path,
                        'fetcher': current_fetcher,
                    }
                )
                processed_nodes_count += 1

        msg = ""
        if total_nodes_count > max_allowed_nodes_count:
            msg = ", nodes count will be limited with max allowed limit: {}".format(max_allowed_nodes_count)

        log.debug('reqKey:{} find_results count:{}, total Nodes Count: {}{}'
                  .format(req_key, find_results_count, total_nodes_count, msg))

        jobs = [
            Job(
                sub.fetch,
                'Fetch values for reqKey={} for {} metrics'.format(
                    req_key, len(sub.nodes)
                ),
                sub.nodes[0].path, req_key, start_time, end_time
            ) for sub in sub_reqs if sub.nodes
        ]
        profiling_time['gen_fetch'] = time.time()

        # Fetch everything in parallel
        if total_nodes_count >= nodes_count_to_use_slow_pool:
            self.prepare_slow_pool(req_key)
        else:
            self.prepare_fast_pool(req_key)

        _ = self.wait_jobs(
            jobs,
            fetch_timeout_seconds,
            'Multifetch for request key {}'.format(req_key)
        )
        profiling_time['fetch'] = time.time()

        for result in results:
            result['time_info'], result['values'] = result['fetcher'].fetch(
                result['path'], req_key, start_time, end_time
            )
        profiling_time['fill'] = time.time()

        log.debug(
            'DEBUG:graphouse_multifetch[{}]: full(unprocessed "yield")={}, find={}, '
            'generetion_fetch_jobs={}, fetch_graphouse_parallel={}, '
            'parse_values={}'.format(
                req_key,
                profiling_time['fill'] - profiling_time['start'],
                profiling_time['find'] - profiling_time['start'],
                profiling_time['gen_fetch'] - profiling_time['find'],
                profiling_time['fetch'] - profiling_time['gen_fetch'],
                profiling_time['fill'] - profiling_time['fetch'],
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
