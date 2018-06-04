import requests
import argparse
import json
import sys

def intersect(a, b):
    """ return the intersection of two lists """
    return list(set(a) & set(b))

class LinkerdClientState(object):
    def _strip_service(self, value):
        return value.split('/')[-1:][0]

    def find_localhost_entries(self):
        path = '/%/io.l5d.localhost'
        return {k:v for k,v in self.data.iteritems() if k.startswith(path)}

    def filter_service_addresses(self, items):
        ret = {}
        for (k, v) in items.iteritems():
            service = self._strip_service(k)
            ret[service] = v.get('addresses', [])
        return ret

class LinkerdClientStateLocal(LinkerdClientState):
    def __init__(self, filename):
        self.filename = filename
        self.data = json.loads(open(self.filename).read())

class LinkerdClientStateRemote(LinkerdClientState):
    def __init__(self, remote_address):
        self.remote_address = remote_address
        self.path = "http://{}/client_state.json".format(remote_address)
        self.data = requests.get(self.path).json()

def consul_list_services(consul):
    path = "http://{}/v1/catalog/services".format(consul)
    r = requests.get(path)
    keys = r.json().keys()
    if 'consul' in keys:
        keys.remove('consul')

    return keys

def consul_list_nodes(consul, service):
    path = "http://{}/v1/catalog/service/{}".format(consul, service)
    r = requests.get(path)
    nodes = [
                {
                    'Node': n.get('Node', None),
                    'Datacenter': n.get('Datacenter', None),
                    'ServiceAddress': n.get('ServiceAddress', None),
                    'ServicePort': n.get('ServicePort', None),
                    'Address': n.get('Address', None),
                } \
            for n in r.json()]
    return nodes

def consul_list_addresses(consul, service):
    nodes = consul_list_nodes(consul, service)
    return ['%s:%s' % (n['Address'], n['ServicePort']) for n in nodes]

def main():
    parser = argparse.ArgumentParser(description='Validate consul state matches linkerd client_state.json')
    parser.add_argument('--consul', dest='consul',
                    default='localhost:8500',
                    help='consul address (default: localhost:8500)')
    parser.add_argument('--linkerd', dest='linkerd',
                    default='localhost:9990',
                    help='linkerd admin (default: localhost:9990)')
    parser.add_argument('--use-linkerd-state-local', dest='linkerd_local',
                    default=None,
                    help='use a local copy of client_state.json instead of remote (default: no)')

    args = parser.parse_args()
    consul = args.consul
    linkerd = args.linkerd
    linkerd_local = args.linkerd_local

    if linkerd_local:
        client_state = LinkerdClientStateLocal(linkerd_local)
    else:
        client_state = LinkerdClientStateRemote(linkerd)

    services = consul_list_services(consul)
    consul_services = {}
    output = {}

    for service in services:
        nodes = consul_list_addresses(consul, service)
        consul_services[service] = nodes

    linkerd_services = client_state.filter_service_addresses(client_state.find_localhost_entries())
    intersection = intersect(linkerd_services.keys(), consul_services.keys())

    for service in intersection:
        for node in linkerd_services[service]:
            if node not in consul_services[service]:
                output[service] = output.get(service, []) + [node]

    # check if any services mismatched, exit 1 if so
    if len(output.keys()):
        print json.dumps(output, indent=4, sort_keys=True)
        print sys.exit(1)

if __name__ == '__main__':
    main()
