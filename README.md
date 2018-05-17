# Linkerd -> Consul state analyzer
This script will query consul for services + nodes, and then validate against Linkerd's `client_state.json` to make sure the routing matches.
