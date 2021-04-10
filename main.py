from Cluster import Cluster
from Workflow import Workflow

async def main():
    # create a compute cluster with 5 nodes
    cloud = Cluster(15, process_speed=25)
    test#1
    # generate a workflow
    wf = Workflow('test workflow')

    # submit the workflow to the cloud
    await cloud.submit_workflow(wf)
    print('[main] done')


from asyncio import run
run(main())
