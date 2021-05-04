from Cluster import Cluster
import asyncio

print('creating a cluster.. use send.py to give it a workflow')

cloud = Cluster()

evloop = asyncio.get_event_loop()
try:
    evloop.run_until_complete(cloud.event_loop())
except KeyboardInterrupt:
    print('')
