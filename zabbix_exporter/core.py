# coding: utf-8
import logging
import re
from collections import OrderedDict

import pyzabbix
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Counter, Gauge, CollectorRegistry

from zabbix_exporter.compat import BaseHTTPRequestHandler
# from zabbix_exporter.commands import cli
from zabbix_exporter.prometheus import MetricFamily, generate_latest
from zabbix_exporter.utils import SortedDict
from loguru import logger

#logger = logging.getLogger(__name__)


exporter_registry = CollectorRegistry()  # makes sure to collect metrics after ZabbixCollector

scrapes_total = Counter('zabbix_exporter_scrapes_total', 'Number of scrapes', registry=exporter_registry)
api_requests_total = Counter('zabbix_exporter_api_requests_total', 'Requests to Zabbix API', registry=exporter_registry)
api_bytes_total = Counter('zabbix_exporter_api_bytes_total', 'Bytes in response from Zabbix API (after decompression)', registry=exporter_registry)
api_seconds_total = Counter('zabbix_exporter_api_seconds_total', 'Seconds spent fetching from Zabbix API', registry=exporter_registry)
metrics_count_total = Gauge('zabbix_exporter_metrics_total', 'Number of exported zabbix metrics', registry=exporter_registry)
series_count_total = Gauge('zabbix_exporter_series_total', 'Number of exported zabbix values', registry=exporter_registry)


def sanitize_key(string):
    return re.sub('[^a-zA-Z0-9:_]+', '_', string)


def prepare_regex(key_pattern):
    return re.escape(key_pattern).replace('\*', '([^,]*?)')


class ZabbixCollector(object):

    def __init__(self, base_url, login, password, verify_tls=True, timeout=None, **options):
        self.options = options
        self.key_patterns = {prepare_regex(metric['key']): metric
                             for metric in options.get('metrics', [])}

        self.zapi = pyzabbix.ZabbixAPI(base_url, timeout=timeout)
        if not verify_tls:
            import requests.packages.urllib3 as urllib3
            urllib3.disable_warnings()
            self.zapi.session.verify = verify_tls

        def measure_api_request(r, *args, **kwargs):
            api_requests_total.inc()
            api_bytes_total.inc(len(r.content))
            api_seconds_total.inc(r.elapsed.total_seconds())
        self.zapi.session.hooks = {'response': measure_api_request}

        self.zapi.login(login, password)

        self.host_mapping = {row['hostid']: row['name']
                             for row in self.zapi.host.get(output=['hostid', 'name'])}

    def process_metric(self, item):
        if not self.is_exportable(item):
            logger.debug('Dropping unsupported metric %s', item['key_'])
            return

        metric = item['key_']
        metric_options = {}
        labels_mapping = SortedDict()
        for pattern, attrs in self.key_patterns.items():
            match = re.match(pattern, item['key_'])
            if match:
                # process metric name
                metric = attrs.get('name', metric)

                def repl(m):
                    asterisk_index = int(m.group(1))
                    return match.group(asterisk_index)
                metric = re.sub('\$(\d+)', repl, metric)

                # ignore metrics with rejected placeholders
                rejected_matches = [r for r in attrs.get('reject', []) if re.search(r, item['key_'])]
                if rejected_matches:
                    logger.debug('Rejecting metric %s (matched %s)', rejected_matches[0], metric)
                    continue  # allow to process metric by another rule

                # create labels
                for label_name, match_group in attrs.get('labels', {}).items():
                    if match_group[0] == '$':
                        label_value = match.group(int(match_group[1]))
                    else:
                        label_value = match_group
                    labels_mapping[label_name] = label_value
                metric_options = attrs
                break
        else:
            if self.options.get('explicit_metrics', False):
                logger.debug('Dropping implicit metric name %s', item['key_'])
                return



        if not self.host_mapping.get(item['hostid']):
            return

        # automatic host -> instance labeling
        labels_mapping['instance'] = self.host_mapping[item['hostid']]

        logger.debug('Converted: %s -> %s [%s]', item['key_'], metric, labels_mapping)
        return {
            'name': sanitize_key(metric),
            'type': metric_options.get('type', 'untyped'),  # untyped by default
            'documentation': metric_options.get('help', item['name']),
            'labels_mapping': labels_mapping,
        }

    def collect(self):
        series_count = 0
        enable_timestamps = self.options.get('enable_timestamps', False)
        metric_families = OrderedDict()

        # Fetch items with basic details
        items = self.zapi.item.get(output=['name', 'key_', 'hostid', 'lastvalue', 'lastclock', 'value_type'],
                                sortfield='key_')

        # Fetch all hosts and their host groups
        host_ids = list(set(item['hostid'] for item in items))
        hosts = self.zapi.host.get(output=['hostid', 'name'],
                                selectGroups=['groupid', 'name'],
                                hostids=host_ids)

        # Create a mapping of host ID to host groups
        host_groups_mapping = {host['hostid']: host['groups'] for host in hosts}

        for item in items:
            metric = self.process_metric(item)
            if not metric:
                continue

            # Get the host group(s) for the current item
            host_groups = host_groups_mapping.get(item['hostid'], [])
            host_group_names = [group['name'] for group in host_groups]

            # Add host group information to the metric's labels
            metric['labels_mapping']['host_group'] = ','.join(host_group_names)

            if metric['name'] not in metric_families:
                family = MetricFamily(typ=metric['type'],
                                    name=metric['name'],
                                    documentation=metric['documentation'],
                                    labels=metric['labels_mapping'].keys())
                metric_families[metric['name']] = family

            metric_families[metric['name']].add_metric(
                metric['labels_mapping'].values(), float(item['lastvalue']),
                int(item['lastclock']) if enable_timestamps else None)
            series_count += 1

        for f in metric_families.values():
            yield f

        metrics_count_total.set(len(metric_families))
        series_count_total.set(series_count)

    def is_exportable(self, item):
        return item['value_type'] in {'0', '3'}  # only numeric/float values


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/metrics':
            try:
                scrapes_total.inc()
                response = generate_latest(REGISTRY)
                status = 200
            except Exception:
                logger.exception('Fetch failed')
                response = b''
                status = 500
            self.send_response(status)
            self.send_header('Content-Type', CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(response)
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'404 Not Found')


# coding: utf-8
import logging

import click
import sys
import yaml
from prometheus_client import REGISTRY

import zabbix_exporter
from zabbix_exporter.core import ZabbixCollector, MetricsHandler
from .compat import HTTPServer

logger = logging.getLogger(__name__)


def validate_settings(settings):
    if not settings['url']:
        click.echo('Please provide Zabbix API URL', err=True)
        sys.exit(1)
    if not settings['login']:
        click.echo('Please provide Zabbix username', err=True)
        sys.exit(1)
    if not settings['password']:
        click.echo('Please provide Zabbix account password', err=True)
        sys.exit(1)
    return True


# @click.command()
# @click.option('--config', help='Path to exporter config',
#               type=click.Path(exists=True))
# @click.option('--port', default=9224, help='Port to serve prometheus stats [default: 9224]')
# @click.option('--url', help='HTTP URL for zabbix instance')
# @click.option('--login', help='Zabbix username')
# @click.option('--password', help='Zabbix password')
# @click.option('--verify-tls/--no-verify', help='Enable TLS cert verification [default: true]', default=True)
# @click.option('--timeout', help='API read/connect timeout', default=5)
# @click.option('--verbose', is_flag=True)
# @click.option('--dump-metrics', help='Output all metrics for human to write yaml config', is_flag=True)
# @click.option('--version', is_flag=True)
# @click.option('--return-server', is_flag=True, help='Developer flag. Please ignore.')
def main(**settings):
    """Zabbix metrics exporter for Prometheus

       Use config file to map zabbix metrics names/labels into prometheus.
       Config below transfroms this:

           local.metric[uwsgi,workers,myapp,busy] = 8
           local.metric[uwsgi,workers,myapp,idle] = 6

       into familiar Prometheus gauges:

           uwsgi_workers{instance="host1",app="myapp",status="busy"} 8
           uwsgi_workers{instance="host1",app="myapp",status="idle"} 6

       YAML:

       \b
           metrics:
             - key: 'local.metric[uwsgi,workers,*,*]'
               name: 'uwsgi_workers'
               labels:
                 app: $1
                 status: $2
               reject:
                 - 'total'
    """
    if settings['version']:
        click.echo('Version %s' % zabbix_exporter.__version__)
        return

    if not validate_settings(settings):
        return

    if settings['config']:
        exporter_config = yaml.safe_load(open(settings['config']))
    else:
        exporter_config = {}

    base_logger = logging.getLogger('zabbix_exporter')
    handler = logging.StreamHandler()
    base_logger.addHandler(handler)
    base_logger.setLevel(logging.ERROR)
    handler.setFormatter(logging.Formatter('[%(asctime)s] %(message)s', "%Y-%m-%d %H:%M:%S"))
    if settings['verbose']:
        base_logger.setLevel(logging.DEBUG)

    collector = ZabbixCollector(
        base_url=settings['url'].rstrip('/'),
        login=settings['login'],
        password=settings['password'],
        verify_tls=settings['verify_tls'],
        timeout=settings['timeout'],
        **exporter_config
    )

    if settings['dump_metrics']:
        return dump_metrics(collector)

    REGISTRY.register(collector)
    httpd = HTTPServer(('', int(settings['port'])), MetricsHandler)
    click.echo('Exporter for {base_url}, user: {login}, password: ***'.format(
        base_url=settings['url'].rstrip('/'),
        login=settings['login'],
        password=settings['password']
    ))
    if settings['return_server']:
        return httpd
    click.echo('Exporting Zabbix metrics on http://0.0.0.0:{}'.format(settings['port']))
    httpd.serve_forever()


def dump_metrics(collector):
    for item in collector.zapi.item.get(output=['name', 'key_', 'hostid', 'lastvalue', 'lastclock', 'value_type'],
                                        sortfield='key_'):
        click.echo('{host:20}{key} = {value}\n{name:>20}'.format(
            host=collector.host_mapping.get(item['hostid'], item['hostid']),
            key=item['key_'],
            value=item['lastvalue'],
            name=item['name']
        ))
    return


# if __name__ == "__main__":

    # cli()
# # 