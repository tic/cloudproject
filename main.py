from Cluster import Cluster
from Workflow import Workflow

async def main():
    # create a compute cluster with 5 nodes
    cloud = Cluster(15, process_speed=25)

    # generate a workflow
    wf = Workflow('test workflow')
    wf2 = Workflow('test workflow 2')

    # submit the workflow to the cloud
    print("one")
    await cloud.submit_workflow(wf)
    #print("two")
    #await cloud.submit_workflow(wf2)
    print('[main] done')


from asyncio import run
run(main())
