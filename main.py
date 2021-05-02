from Cluster import Cluster
import asyncio

cloud = Cluster()

evloop = asyncio.get_event_loop()
try:
    evloop.run_until_complete(cloud.event_loop())
except KeyboardInterrupt:
    print('')
