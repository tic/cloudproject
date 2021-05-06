# Metrics used to measure the performance of scheduling algorithms.
from statistics import mean
import pandas


def NCT(task_df, wf):
    # Takes a task_df and wf object as input
    #print("Calculating NCT")
    #print(task_df)
    #print(wf)
    #print(wf.name)
    rdf = task_df.taskdf[task_df.taskdf['workflow']==wf.name]
    #print('Workflow in Task dataframe has been found')
    ct_wf = rdf['completion_time'].max()
    #print('latest completion time of workflow found')
    NCT = (ct_wf - wf.arrival_time) / (rdf.minimum_runtime.sum())
    #print("NCT has been calculated")
    return NCT

def ANCT(task_df, wf_array):
    # Takes a task_df object and list of wf objects as inputs
    print("Calculating ANCT")
    return mean([NCT(task_df, wf) for wf in wf_array])

def TC(service_instances):
    # Takes a list of service instances as input
    from Task import crt
    current_time = crt()
    print("Calculating TC")
    #node.provisioned time is the clock time for when a node was provisioned
    return mean([node.get_live_provisioned_time() * node.cost / 3600 for node in service_instances])

def RU(service_instances):
    print("Calculating RU")
    total_working_time = sum([node.execution_time for node in service_instances])
    print([node.execution_time for node in service_instances])
    total_active_time = sum([node.get_live_provisioned_time() for node in service_instances])
    print([node.get_live_provisioned_time() for node in service_instances])
    return total_working_time / total_active_time
