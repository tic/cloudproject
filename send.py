# This script will generate a workflow and submit it to an already-running cluster for processing.
import socket
import Workflow
import json

port_offset = int(input('port offset: '))
do_wf = input('press enter to send a workflow, or any key\nthen enter to trigger the sanity check: ')


try:
    if do_wf == '':
        print('generating workflow')
        wf = Workflow.generate_workflow()
        msg = json.dumps(wf) + '\x00'

    elif do_wf == 't':
        wf = None
        with open('wf_test.json', 'r') as file:
            wf = json.loads(file.read())
        msg = json.dumps(wf) + '\x00'

    elif do_wf.lower() == "m" or do_wf.lower() == "e" or do_wf.lower() == "s":
        print('generating specified workflow')
        wf = Workflow.generate_workflow(do_wf.lower())
        msg = json.dumps(wf) + '\x00'

    elif do_wf == "metrics":
        msg = '\x01'

    else:
        msg = '\x02'

    print('connecting to cluster')
    server = socket.create_connection(('127.0.0.1', 15555 + port_offset))

    print('sending to cluster')
    server.send(msg.encode('utf-8'))
finally:
    print('closing connection')
    server.close()
