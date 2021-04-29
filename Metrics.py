# Metrics used to measure the performance of scheduling algorithms.
from statistics import mean


def NCT(task_df, wf):
    # Takes a task_df and wf object as input
    rdf = task_df[task_df['workflow']==wf.name]
    ct_wf = rdf['completion time'].max()
    NCT = (wf.arrival_time) / (rdf.minimum_runtime.sum())
    return NCT

def ANCT(task_df, wf_array):
    # Takes a task_df object and list of wf objects as inputs
    return mean([NCT(task_df, wf) for wf in wf_array])

def TC(service_instances):
    # Takes a list of service instances as input
    return mean([node.provisioned_time * node.cost for node in service_instances])

def RU(service_instances):
    total_working_time = sum([node.execution_time for node in service_instances])
    total_active_time = sum([node.provisioned_time for node in service_instances])
    return total_working_time / total_active_time
