from Cluster import Cluster
import asyncio

print('creating a cluster.. use send.py to give it a workflow')

cloud = Cluster(scheduler_type="fifo", node_list = [5,5,5,5])

evloop = asyncio.get_event_loop()
try:
    evloop.run_until_complete(cloud.event_loop())
except KeyboardInterrupt:
    print('')
