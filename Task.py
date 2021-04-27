
from asyncio import sleep
import pandas
import json

from time import time as now
crt = lambda : round(now() * 1000)

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

    # @wf(dict) - dictionary representation of workflow json
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

    # returns a pd series representation of single task
    # @task(string) - the task's name
    def get_task_row(self, task):
        # might want to check to see if there are no duplicate names before squeezing
        return self.taskdf[self.taskdf['name'] == task].squeeze()

    # Completion Time
    # @task(string) - the task's name
    def ct(self, task):
        
        taskobj = self.get_task_row(task)

        json_time = taskobj.minimum_runtime
        it = self.it(task)

        task_parents = self.get_task_row(task).parents
        #TODO: possible performance issues due to recursive call to ct. Should we store this data in the dataframe?
        parent_max_ct = max([self.ct(t) for t in task_parents]) if task_parents else 0

        return json_time + it + parent_max_ct

    ## TODO: finish this function
    # @task(string) - the task's name
    def lct(self, task):

        task_df = self.taskdf[self.taskdf.name == task]

        if not task_df.empty:

            task_children = task_df.squeeze().children

            if task_children:
                # find all the parents of a node with name 'filterContams_00000002'
                #self.taskdf[self.taskdf.parents.apply(lambda x: 'filterContams_00000002' in x)]

                parents_of_children = self.taskdf[self.taskdf.name.isin(task_children)]

                # calculate completion time of all parents_of_children
                
            else:
                return 0

    # Input Time -- the time it takes a task to read in its files
    # @task(string) - the task's name
    def it(self, task):
        from Node import Node

        taskobj = self.get_task_row(task)

        srv_id = taskobj.service_instance_id # returns None if no service instance is found

        # sums the size of all inputs files together
        input_size = sum([f['size'] for f in taskobj.files if f['link'] == 'input'])

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
