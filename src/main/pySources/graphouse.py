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
        Job, get_pool, pool_exec, PoolTimeoutError
    )
    from graphite.finders.utils import BaseFinder
except ImportError:
    # Backward compatibility with older than 1.1
    BaseFinder = object
    log.debug = log.warning = log.info


graphouse_url = getattr(settings, 'GRAPHOUSE_URL', 'http://localhost:2005')
# See graphouse.http.max-form-context-size-bytes property
max_data_size = getattr(settings, 'GRAPHOUSE_MAX_FORM_SIZE', 500000)
GRAPHITE_VERSION = tuple(
    [int(i) for i in getattr(settings, 'WEBAPP_VERSION').split('.')]
)


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
            paths = [node.path.replace("'", "\\'") for node in self.nodes]

            query = {
                        'start': start_time,
                        'end': end_time,
                        'reqKey': reqkey
                    }
            data = {'metrics': ','.join(paths)}
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
            log.exception("CRITICAL:graphouse_data_query: Connection error: {}"
                          .format(str(e)))
            raise
        except requests.exceptions.HTTPError as e:
            log.exception("CRITICAL:graphouse_data_query: {}, message: {}"
                          .format(str(e), request.text))
            raise
        except Exception:
            log.exception("Unexpected exception!", exc_info=True)
            raise

        profilingTime['fetch'] = time.time()

        try:
            metrics_object = request.json()
        except Exception:
            log.exception(
                "CRITICAL:graphouse_parse: "
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
                            metric_object.get("step")),
                        metric_object.get("points"),
                    )

        profilingTime['convert'] = time.time()

        log.debug(
            'DEBUG:graphouse_time:[{}] full = {}'
            ' fetch = {}, parse = {}, convert = {}'.format(
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
                "WARNING:graphouse_data: something strange:"
                " path {} doesn't exist in graphouse response".format(path)
            )
            raise ValueError(
                "path {} doesn't exist in graphouse response".format(path)
            )
        else:
            return result


class GraphouseFinder(BaseFinder):
    def _search_request(self, pattern):
        request = requests.post(
            url='{}/search'.format(graphouse_url),
            data={'query': pattern}
        )
        request.raise_for_status()
        result = (pattern, request.text.split('\n'))
        return result

    def _wait_jobs(self, jobs, timeout, context, thread_count=2):
        '''
        Parallel asynchronous execution of jobs.
        This methode were copied from graphite.storage
        '''
        if not jobs:
            return []

        pool_name = "graphouse"
        if settings.USE_WORKER_POOL:
            thread_count = min(thread_count, settings.POOL_MAX_WORKERS)
        else:
            thread_count = 0
        pool = get_pool(name=pool_name, thread_count=thread_count)
        results = []
        failed = []
        done = 0
        start = time.time()
        try:
            for job in pool_exec(pool, jobs, timeout):
                done += 1
                elapsed = time.time() - start
                if job.exception:
                    failed.append(job)
                    log.info("Exception during {} after {}s: {}".format(
                        job, elapsed, str(job.exception))
                    )
                else:
                    log.debug("Got a result for {} after {}s"
                              .format(job, elapsed))
                    results.append(job.result)
        except PoolTimeoutError:
            message = "Timed out after {}s for {}".format(
                time.time() - start, context
            )
            log.info(message)
            if done == 0:
                raise Exception(message)

        if len(failed) == done:
            message = "All requests failed for {} ({})".format(
                context, len(failed)
            )
            for job in failed:
                message += "\n\n{}: {}: {}".format(
                    job, job.exception,
                    '\n'.join(traceback.format_exception(*job.exception_info))
                )
            raise Exception(message)

        if len(results) < len(jobs) and settings.STORE_FAIL_ON_ERROR:
            message = "{} request(s) failed for {} ({})".format(
                len(jobs) - len(results), context, len(jobs)
            )
            for job in failed:
                message += "\n\n{}: {}: {}".format(
                    job, job.exception,
                    '\n'.join(traceback.format_exception(*job.exception_info))
                )
            raise Exception(message)

        return results

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
                yield LeafNode(metric,
                               GraphouseReader(metric, fetcher=fetcher))

    def find_multi(self, patterns, reqkey=None):
        '''
        This method processed in graphite 1.1 and newer from self.fetch
        '''
        reqkey = reqkey or uuid.uuid4()
        jobs = [
            Job(self._search_request,
                'Query graphouse for {}'.format(pattern), pattern)
            for pattern in patterns
        ]

        results = [
            result for result in self._wait_jobs(
                jobs, getattr(settings, 'FIND_TIMEOUT'),
                'Find nodes for {} request'.format(reqkey)
            )
        ]

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

        log.debug('Multifetcher, patterns={}'.format(patterns))
        profilingTime = {'start': time.time()}

        requestContext = requestContext or {}
        reqkey = uuid.uuid4()
        results = []

        find_results = self.find_multi(patterns, reqkey)
        profilingTime['find'] = time.time()
        log.debug('Results from find_multy={}'
                  .format([(f[0], f[1]) for f in find_results]))

        body_overhead = len('metrics')
        subreq_len = 0
        subreqs = [GraphouseMultiFetcher()]

        subreq = 0
        for pair in find_results:
            pattern = pair[0]
            nodes = pair[1]
            for node in nodes:
                if not isinstance(node, LeafNode):
                    continue
                node_len = len(node.path) + 1  # additional coma or equal sign
                body_size = subreq_len + node_len + body_overhead
                if body_size <= max_data_size:
                    subreqs[subreq].append(node)
                    subreq_len += node_len
                else:
                    subreqs.append(GraphouseMultiFetcher())
                    subreq += 1
                    subreqs[subreq].append(node)
                    subreq_len = node_len
                results.append(
                    {
                        'pathExpression': pattern,
                        'name': node.path,
                        'path': node.path,
                        'fetcher': subreqs[subreq],
                    }
                )

        jobs = [
            Job(
                sub.fetch,
                'Fetch values for reqkey={} for {} metrics'.format(
                    reqkey, len(sub.nodes)
                ),
                sub.nodes[0].path, reqkey, start_time, end_time
            ) for sub in subreqs
        ]
        profilingTime['gen_fetch'] = time.time()

        # Fetch everything in parallel
        _ = self._wait_jobs(jobs, getattr(settings, 'FETCH_TIMEOUT'),
                            requestContext)
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
