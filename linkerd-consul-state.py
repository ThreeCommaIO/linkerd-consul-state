import requests
import argparse
import json
import sys

def linkerd_client_state(linkerd):
    path = "http://{}/client_state.json".format(linkerd)
    r = requests.get(path)
    return r.json()

def linkerd_client_state_local(file):
    return json.loads(open(file).read())

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
                    'Node': n['Node'],
                    'Datacenter': n['Datacenter'],
                    'ServiceAddress': n['ServiceAddress'],
                    'ServicePort': n['ServicePort'],
                    'Address': n['Address'],
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
        client_state = linkerd_client_state_local(linkerd_local)
    else:
        client_state = linkerd_client_state(linkerd)
    services = consul_list_services(consul)
    consul_services = {}
    output = {}

    for service in services:
        nodes = consul_list_addresses(consul, service)
        consul_services[service] = nodes

    linkerd_services = client_state.keys()
    for (consul_service, consul_addresses) in consul_services.iteritems():
        matching = [s for s in linkerd_services if consul_service in s]
        if matching:
            linkerd_service = matching[0]
            linkerd_addresses = client_state[linkerd_service]['addresses']
            differences = set(consul_addresses) - set(linkerd_addresses)
            if differences:
                output[linkerd_service] = dict(linkerd=list(set(linkerd_addresses)), consul=list(set(consul_addresses)))

    print json.dumps(output, indent=4, sort_keys=True)

    # check if any services mismatched, exit 1 if so
    if len(output.keys()):
        print sys.exit(1)

if __name__ == '__main__':
    main()
