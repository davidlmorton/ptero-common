from .base import Janitor
from . import exceptions

from urlparse import urljoin, urlunparse
from ptero_common import nicer_logging
import requests
import urllib


LOG = nicer_logging.getLogger(__name__)


class RabbitMQJanitor(Janitor):
    ALLOWED_SCHEMES = {'amqp'}

    IGNORED_EXCHANGE_NAMES = {
        '',
        'amq.direct',
        'amq.fanout',
        'amq.headers',
        'amq.match',
        'amq.rabbitmq.log',
        'amq.rabbitmq.trace',
        'amq.topic',
    }

    def __init__(self, url, api_port=15672):
        super(RabbitMQJanitor, self).__init__(url)
        self.base_api_url = urlunparse(
            ('http', '%s:%s' % (self.url_obj.hostname, api_port),
             '', '', '', ''))

    def clean(self):
        LOG.debug('Beginning purge of %s', self.sanitized_url)
        self.kill_connections()
        self.delete_queues()
        self.delete_exchanges()

    def kill_connections(self):
        LOG.debug('Force quitting connections at %s', self.sanitized_url)
        for connection_name in self.connection_names():
            self.kill_connection(connection_name)

        if self.has_connections():
            LOG.error('Found unexpected remaining connections at %s: %s',
                      self.sanitized_url, self.connection_names())
            raise exceptions.JanitorException()

    def kill_connection(self, connection_name):
        LOG.debug('Killing connection %s at %s',
                  connection_name, self.sanitized_url)
        self.api_delete('connections', urllib.quote(connection_name))

    def delete_queues(self):
        LOG.debug('Deleting queues at %s', self.sanitized_url)

        for queue_name in self.queue_names():
            self.delete_queue(queue_name)

        if self.has_queues():
            LOG.error('Found unexpected remaining queues at %s: %s',
                      self.sanitized_url, self.queue_names())
            raise exceptions.JanitorException()

    def delete_queue(self, queue_name):
        LOG.debug('Deleting queue %s at %s', queue_name, self.sanitized_url)
        self.api_delete('queues', self.vhost, queue_name)

    def delete_exchanges(self):
        LOG.debug('Deleting exchanges at %s', self.sanitized_url)
        for exchange_name in self.exchange_names():
            self.delete_exchange(exchange_name)

        if self.has_exchanges():
            LOG.error('Found unexpected remaining exchanges at %s: %s',
                      self.sanitized_url, self.exchange_names())
            raise exceptions.JanitorException()

    def delete_exchange(self, exchange_name):
        LOG.debug('Deleting exchanges at %s', self.sanitized_url)
        self.api_delete('exchanges', self.vhost, exchange_name)

    def has_connections(self):
        return len(self.connection_names()) > 0

    def has_queues(self):
        return len(self.queue_names()) > 0

    def has_exchanges(self):
        return len(self.exchange_names()) > 0

    def connection_names(self):
        return [c['name']
                for c in self.api_get('connections')
                if c['vhost'] == self.vhost]

    def queue_names(self):
        return [q['name'] for q in self.api_get('queues', self.vhost)]

    def exchange_names(self):
        return [e['name'] for e in self.api_get('exchanges', self.vhost)
                if e['name'] not in self.IGNORED_EXCHANGE_NAMES]

    @property
    def vhost(self):
        return self.url_obj.path.split('/')[-1]

    def api_get(self, *parts):
        return self.api_request('get', *parts)

    def api_delete(self, *parts):
        return self.api_request('delete', *parts)

    def api_request(self, method, *parts):
        method = getattr(requests, method)
        response = method(self.api_url(*parts), auth=self.api_auth())

        if int(response.status_code / 100) != 2:
            LOG.error(
                'Got unexpected response code (%s) from url %s: %s',
                response.status_code, self.api_url(*parts), response.text)
            raise exceptions.JanitorException()

        if response.status_code == 204:
            return
        else:
            return response.json()

    def api_url(self, *parts):
        return urljoin(self.base_api_url, self.api_path('api', *parts))

    def api_path(self, *parts):
        return '/' + '/'.join(parts)

    def api_auth(self):
        return (self.username, self.password)

    @property
    def username(self):
        if self.url_obj.username:
            return self.url_obj.username
        else:
            return 'guest'

    @property
    def password(self):
        if self.url_obj.password:
            return self.url_obj.password
        else:
            return 'guest'
