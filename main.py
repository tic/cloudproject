from Cluster import Cluster

# create a compute cluster with 5 nodes
cloud = Cluster(5)

# generate a workflow
wf = Workflow('test workflow')

# submit the workflow to the cloud
cloud.submit_workflow(wf)
