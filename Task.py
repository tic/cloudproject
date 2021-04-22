
from asyncio import sleep
import pandas
import json

from time import time as now
crt = lambda : round(now() * 1000)

class WorkingTask(Task):
    def __init__(self, *args, **kwargs):
        pass


class Tasks(object):

    def __init__(self):
        self.taskdf = pandas.DataFrame(columns=[
            'name',
            'workflow',
            'parents',
            'children',
            'files',
            'service_instance_id', # the service instance id that this task will be run on
            'minimum_runtime',
            'start_time', # start time of the task
            'completion_time', # calculated an used in Algorithm 2
            'predicated_completion_time', # used in Algorithm 1 to sort the tasks_st
            'latest_completion_time', # Used in Algorithm 2. Calcualted by using equation 8
            'complete',
        ])

        self.edgedf = pandas.DataFrame(columns=[
            'tasks', # should be in a standardized format like <NAME>,<NAME>
            'prevtask',
            'nexttask',
        ])

    def add_tasks_from_wf(self, wf):

        wf_name = wf.name
        for task in wf.get_task_json():
            self.taskdf = self.taskdf.append({
                'name': task['name'],
                'workflow': wf_name,
                'parents': task['parents'],
                'children': task['children'],
                'files': task['files'],
                'service_instance_id': None,
                'minimum_runtime': task['runtime'],
                'start_time': float('inf'),
                'complete': False,
            }, ignore_index=True)


            # the next two methods may cause issues: there is no check in place to determin if the child task exists
            # we're relying on the imported workframe data to be pristine

            # create edges to connect parent tasks
            self.edgedf = self.edgedf.append([
                {
                    'tasks': ','.join([p, task['name']]),
                    'prevtask': task['name'],
                    'nexttask': p
                } for p in task['parents']
            ])

            # create edges to connect child tasks
            self.edgedf = self.edgedf.append([
                {
                    'tasks': ','.join([task['name'], c]),
                    'prevtask': task['name'],
                    'nexttask': c
                } for c in task['children']
            ])

    def lct(t_name):

        pass







    # Input Time -- the time it takes a task to read in its files
    # @task(string) - the task's name
    def it(self, task):
        from Node import Node
        srv_id = self.tasksdf[self.tasksdf['name'] == task]['service_instance_id']
        input_size = 0 # TODO
        try:
            mapped_node = Node.instance_map[srv_id]
        except KeyError:
            mapped_node = None
        if mapped_node is None:
            # Assume the worse case node
            from Node import node_types
            wc_proc, wc_read, wc_write = node_types[len(node_types) - 1]

            # input time is the size of the input divided by the worse case read time
            return input_size / wc_read

        # Task has a mapped node, use it's actual speed data
        rs = mapped_node.read_speed # TODO - This property does not exist in the node class, yet...
        return input_size / rs

    # Output Time -- the time it takes a task to write its output files
    # @task(string) - the task's name
    def ot(self, task):
        pass

    # Data Transfer Time (dt) from task p to task j
    # @task_p(string) - the task's name
    # @task_j(string) -    "       "
    def dt(self, task_p, task_j):
        # if tasks  and j are on the same service instance,
        # the data transfer time is zero
        if self.taskdf[self.taskdf['name'] == task_p]['service_instance_id'] == self.taskdf[self.taskdf['name'] == task_j]['service_instance_id']:
            return 0

        return ot(task_p) + it(task_j)
