import itertools
import time
import json

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

import requests

HEADERS = {'content-type': 'application/json'}

class CyaniteLeafNode(LeafNode):
    __fetch_multi__ = 'cyanite'


class URLs(object):
    def __init__(self, hosts):
        self.iterator = itertools.cycle(hosts)

    @property
    def host(self):
        return next(self.iterator)

    @property
    def paths(self):
        return '{0}/paths'.format(self.host)

    @property
    def metrics(self):
        return '{0}/metrics'.format(self.host)
urls = None


class CyaniteReader(object):
    __slots__ = ('path', 'tenant',)

    def __init__(self, path, tenant):
        self.path = path
        self.tenant = tenant

    def fetch(self, start_time, end_time):
        resp = requests.post(urls.metrics, data=json.dumps({'path': self.path,
                                                  'from': start_time,
                                                  'to': end_time,
                                                  'tenant': self.tenant}), headers = HEADERS)
        data = resp.json()
        resp.close()
        if 'error' in data:
            return (start_time, end_time, end_time - start_time), []
        time_info = data['from'], data['to'], data['step']
        return time_info, data['series'].get(self.path, [])

    def get_intervals(self):
        # TODO use cyanite info
        start = time.time() - 3600 * 2
        end = max(start, time.time())
        return IntervalSet([Interval(start, end)])


class CyaniteFinder(object):
    __fetch_multi__ = 'cyanite'

    def __init__(self, config=None):
        global urls
        if config is not None:
            if 'urls' in config['cyanite']:
                urls = config['cyanite']['urls']
            else:
                urls = [config['cyanite']['url'].strip('/')]
        else:
            from django.conf import settings
            urls = getattr(settings, 'CYANITE_URLS')
            if not urls:
                urls = [settings.CYANITE_URL]
        urls = URLs(urls)

    def find_nodes(self, query, tenant):
        resp = requests.post(urls.paths,
                             data=json.dumps({'query': query.pattern, 'tenant': tenant}), headers = HEADERS)
        paths = resp.json()
        resp.close()
        for path in paths:
            if path['leaf']:
                yield CyaniteLeafNode(path['path'],
                                      CyaniteReader(path['path'], tenant))
            else:
                yield BranchNode(path['path'])

    def fetch_multi(self, nodes, tenant, start_time, end_time):
        paths = [node.path for node in nodes]
        resp = requests.post(urls.metrics, data=json.dumps({'path': paths,
                                                  'from': start_time,
                                                  'to': end_time,
						  'tenant': tenant}),headers = HEADERS)
        data = resp.json()
        resp.close()
        if 'error' in data:
            return (start_time, end_time, end_time - start_time), {}
        time_info = data['from'], data['to'], data['step']
        return time_info, data['series']
