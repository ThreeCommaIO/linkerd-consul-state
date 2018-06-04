#!/usr/bin/env python
"""
Export LinkerdStateConsul using stats backend(s):
 * prometheus

"""
from linkerd_state import LinkerdStateConsul, LinkerdClientStateLocal, LinkerdClientStateRemote
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import argparse
import os

class PromHTTPServer(HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, client_state, consul, bind_and_activate=True):
        HTTPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate)
        self.client_state = client_state
        self.consul = consul

class PromHandler(BaseHTTPRequestHandler):
    def do_GET(self):

        state = LinkerdStateConsul(self.server.client_state, self.server.consul)
        output = state.analyze()

        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()

        self.wfile.write('# TYPE linkerd_client_state_mismatch gauge\n')
        self.wfile.write('linkerd_client_state_mismatch %s\n' % str(len(output.keys())))
        return

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
    parser.add_argument('--prom-listen-addr', dest='prom_listen_addr',
                    default='127.0.0.1', type=str,
                    help='prometheus metrics port (default: 127.0.0.1)')
    parser.add_argument('--prom-listen-port', dest='prom_listen_port',
                    default=9985, type=int,
                    help='prometheus metrics port (default: 9985)')

    args = parser.parse_args()
    consul = args.consul
    linkerd = args.linkerd
    linkerd_local = args.linkerd_local

    prom_listen_addr = os.environ.get('PROMETHEUS_STATS_ADDR', args.prom_listen_addr)
    prom_listen_port = int(os.environ.get('PROMETHEUS_STATS_PORT', args.prom_listen_port))

    if linkerd_local:
        client_state = LinkerdClientStateLocal(linkerd_local)
    else:
        client_state = LinkerdClientStateRemote(linkerd)


    try:
        server = PromHTTPServer((prom_listen_addr, prom_listen_port), PromHandler, client_state, consul)
        print "Webserver is now running on port %d" % (prom_listen_port)
        server.serve_forever()
    except KeyboardInterrupt:
        print '^C received, shutting down the web server'
        server.socket.close()

if __name__ == '__main__':
    main()
