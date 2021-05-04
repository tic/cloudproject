# This script will generate a workflow and submit it to an already-running cluster for processing.
import socket
import Workflow
import json

do_wf = input('press enter to send a workflow, or any key\nthen enter to trigger the sanity check: ') == ''


try:
    if do_wf:
        print('generating workflow')
        wf = Workflow.generate_workflow()
        msg = json.dumps(wf) + '\x00'
    else:
        msg = '\x01'

    print('connecting to cluster')
    server = socket.create_connection(('127.0.0.1', 15555))

    print('sending to cluster')
    server.send(msg.encode('utf-8'))
finally:
    print('closing connection')
    server.close()
