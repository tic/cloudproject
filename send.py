# This script will generate a workflow and submit it to an already-running cluster for processing.
import socket
import Workflow
import json

print('generating workflow')
wf = Workflow.generate_workflow()

try:
    print('connecting to cluster')
    server = socket.create_connection(('127.0.0.1', 15555))

    print('sending to cluster')
    wf_string = json.dumps(wf) + '\x00'
    server.send(wf_string.encode('utf-8'))
finally:
    print('closing connection')
    server.close()
