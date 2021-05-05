
from asyncio import sleep
import pandas
import json
import random
import string
import math

from time import time as now
crt = lambda : round(now() * 1000) / 1000 # get current time to the nearest millisecond

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
            'parent_count',
            'unmapped_parent_count', # the number of parents who are not yet mapped to a service instance node. Used for Algorithm 1
            'complete',
            'scheduled',
        ])
        self.taskdf.set_index('name', inplace=True)

        self.edgedf = pandas.DataFrame(columns=[
            'tasks', # should be in a standardized format like <NAME>,<NAME>
            'prevtask',
            'nexttask',
        ])

    # @wf(dict) - dictionary representation of workflow json
    def add_tasks_from_wf(self, wf):

        wf_name = wf.name
        for task in wf.get_task_json():
            self.taskdf = self.taskdf.append(pandas.Series({
                #'name': task['name'],
                'workflow': wf_name,
                'parents': ['_'.join([t, wf_name]) for t in task['parents']],
                'children': ['_'.join([t, wf_name]) for t in task['children']],
                'parent_count': len(task['parents']),
                'unmapped_parent_count': len(task['parents']),
                'files': task['files'],
                'service_instance_id': None,
                'minimum_runtime': task['runtime'],
                'start_time': float('inf'),
                'complete': False,
                'scheduled': float('inf'),
            }, name='_'.join([task['name'], wf_name])))


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

    def get_tasks(self, completed=False, mapped=None):

        rdf = self.taskdf

        if completed != None:
            rdf = rdf[rdf['complete'] == completed]

        if mapped == True:
            rdf = rdf[rdf['service_instance_id'] != None]
        elif mapped == False:
            rdf = rdf[rdf['service_instance_id'].isnull()]

        return rdf

    # returns a pd series representation of single task
    # @task(string) - the task's name
    def get_task_row(self, task):
        # might want to check to see if there are no duplicate names before squeezing
        return self.taskdf.loc[task].squeeze()

    def update_task_field(self, task, field, value):
        from collections.abc import Iterable

        if isinstance(task, str):
            self.taskdf.loc[task, field] = value
        elif isinstance(task, Iterable):
            self.taskdf.loc[self.taskdf.index.isin(task), field] = value

    # duplicates a task. Appends a textstring to the name
    # @task(string) - the task's name
    def duplicate_task(self, task):
        taskobj = self.get_task_row(task)

        # string to append to the end of the task name. Randomly generated letters and number of size n
        n = 5
        append_string = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(n))
        taskobj.name = taskobj.name + '_' + append_string
        self.taskdf = self.taskdf.append(taskobj)

        return taskobj.name

    # unmaps tasks to service instances of all incomplete tasks
    # also resets the 'unmapped_parent_count' field to equal the parent count
    def unmap_service_instances(self):
        self.taskdf.loc[self.taskdf.complete == False, 'service_instance_id'] = None
        self.taskdf.loc[self.taskdf.complete == False, 'scheduled'] = float('inf')
        self.taskdf.loc[self.taskdf.complete == False, 'unmapped_parent_count'] = self.taskdf.loc[self.taskdf.complete == False, 'parent_count']
        self.taskdf.loc[self.taskdf.complete == False, 'completion_time'] = None

    # Get next task for a particular service instance
    # Returns only the name of the next task
    def get_next_task(self, si):
        matches = self.taskdf[(self.taskdf.service_instance_id == si) & (self.taskdf.complete == False)]
        if len(matches) == 0:
            return None

        # Find the min. scheduled task
        min_task = matches[matches.scheduled == matches.scheduled.min()]
        min_task = min_task.head(1).squeeze().name
        #print("finding next task")
        return min_task


    # If a node is about to run a task, this function makes sure that it can't run until
    # all predecessors have been run and data transfered    
    async def wait_to_run(self, task, srv_id):
        #print("checking to see if we can run")
        parents = self.get_task_row(task).parents
        #print("parents are ", parents)
        flag = True
        while flag:
            #print("top of loop")
            flag1 = False
            for p in parents:
                #print("instrumentation parents")
                parentobj = self.get_task_row(p)
                if parentobj.complete == False:
                    flag1 = True
                    break
            #print("instrumentation 1")
            current_time = crt()
            if flag1:
                flag = True
            else:
                #print("flag1 is ", flag1)
                flag2 = False
                for p in parents:
                    parentobj = self.get_task_row(p)
                    if parentobj.completion_time + self.dt(p, task, srv_id) > current_time:
                        flag2 = True
                        break
                if flag2:
                    flag = True
                else:
                    flag = False
            #print("flag is ", flag)
            await sleep(0.00000000001)
        #print("out of loop")

        return 0



    # Get the earliest time that a task can run
    def get_earliest_start_time(self, task, srv_id=None, hyp_node_type=None):
        parents = self.get_task_row(task).parents
        earliest_start_time = 0
        slowest_parent = None
        for p in parents:

            # if the current task has not been mapped
            if self.get_task_row(task).service_instance_id is None:

                # Calculating start time when no hypothetical node type is provided
                if hyp_node_type is None:

                    # assume the worst case
                    if  srv_id is None:
                        start_time = self.get_task_row(p).completion_time + self.dt(p, task)
                    else:
                        start_time = self.get_task_row(p).completion_time + self.dt(p, task, srv_id)

                # Used in 2nd part of TaskSchedule algorithm
                else:
                    start_time = self.get_task_row(p).completion_time + self.dt(p, task, hyp_node_type=hyp_node_type)

            # The current task has already been mapped to a service instance
            else:
                start_time = self.get_task_row(p).completion_time + self.dt(p, task, self.get_task_row(task).service_instance_id)

            slowest_parent = p if start_time < earliest_start_time else slowest_parent
            earliest_start_time = start_time if start_time <  earliest_start_time else earliest_start_time

        return earliest_start_time, slowest_parent

    # given a task name string, decrements the 'unmapped_parent_count' field for all children taskss
    # @task(string) - the task's name
    def signal_children_si_mapped(self, task):
        self.taskdf.loc[self.taskdf.parents.apply(lambda x: task in x), 'unmapped_parent_count'] -= 1

    # Completion Time
    # @task(string) - the task's name
    # Completion Time
    # @task(string) - the task's name
    def ct(self, task, curr_node=None, duplicated_tasks=None, hyp_node_type=None):
        from Node import Node, node_types
        taskobj = self.get_task_row(task)
        if taskobj.service_instance_id is not None:
            node_type = Node.instance_map[taskobj.service_instance_id].ntype

        # There's a specific node being looked at when calculating CT in most cases, hence the following
        if curr_node is None:
            node_type = len(node_types) - 1
        else:
            node_type = Node.instance_map[curr_node].ntype

        if not hyp_node_type is None:
            node_type = hyp_node_type

        json_time = taskobj.minimum_runtime / node_types[node_type][0]

        task_parents = self.get_task_row(task).parents

        #This calculates the latest time at which a predecessor completes and finishes the data transfer process taking into account the current node
        parent_max_ct_dt = max([self.get_task_row(t)['completion_time'] + self.dt(t, task, curr_node, node_type) for t in task_parents]) if task_parents else 0

        if not curr_node is None:
            if not self.taskdf[self.taskdf.service_instance_id == curr_node]['completion_time'].empty:
                curr_node_earliest_finish_time = self.taskdf[self.taskdf.service_instance_id == curr_node]['completion_time'].max()
            else:
                curr_node_earliest_finish_time = 0
        else:
            curr_node_earliest_finish_time = 0

        # min function is used in case for task duplication bc no need to wait for parents on different SI to finish
        # max function used when no task duplication bc the soonest you can start is limited by which completes first
        if not duplicated_tasks is None:
            curr_node_earliest_finish_time += sum([self.get_task_row(t).minimum_runtime / node_types[node_type][0] for t in duplicated_tasks])
            curr_node_earliest_finish_time += sum([self.it(p, t, curr_node, node_type) for t in duplicated_tasks for p in self.get_task_row(t).parents])
            earliest_start_time = min(parent_max_ct_dt, curr_node_earliest_finish_time)
        else:
            earliest_start_time = max(parent_max_ct_dt, curr_node_earliest_finish_time)
        return json_time + earliest_start_time


    ## TODO: finish this function
    # @task(string) - the task's name
    def lct(self, task):

        taskobj = self.get_task_row(task)

        task_children = taskobj.children

        if task_children:
            return min(
                max(self.ct(p, self.get_task_row(p).service_instance_id) for p in self.get_task_row(child).parents) - self.dt(task, child) for child in task_children
            )
        else:
            return 0

    # Input Time -- the time it takes a task to read in its files
    # @task(string) - the task's name
    def it(self, parent, task, srv_id=None, hyp_node_type=None):
       #no read time if parent and task are on same node
        parent_srv_id = self.get_task_row(parent)['service_instance_id']
        if not parent_srv_id is None:
            if parent_srv_id == srv_id:
                return 0

        #get size of files to be read in from parent
        parent_output_names = [f['name'] for f in self.get_task_row(parent)['files']]
        task_input = self.get_task_row(task)['files']
        input_size = sum(f['size'] for f in task_input if f['name'] in parent_output_names)

        # if a hypothetical node type is passed in, then use that to calculate read speed
        if not hyp_node_type is None:
            from Node import node_types
            wc_read= node_types[hyp_node_type][1]
            return input_size / wc_read

        #assume worst case scenario - we don't know what service instance the task is considering
        if srv_id is None:
            # No mapped node, assume the worst case node
            from Node import node_types
            wc_read= node_types[len(node_types) - 1][1]

            # input time is the size of the input divided by the worse case read time
            return input_size / wc_read

        else:
            from Node import Node
            candidate_node = Node.instance_map[srv_id]

            # calculate read time based on read speed of current node
            return input_size / candidate_node.read_speed


    # Output Time -- the time it takes a task to write its output files
    # @task(string) - the task's name
    def ot(self, task):
        taskobj = self.get_task_row(task)
        srv_id = taskobj.service_instance_id # returns None if no service instance is found

        # Compute output size by summing the size of all inputs files
        output_size = sum([f['size'] for f in taskobj.files if f['link'] == 'output'])

        # Get the node the task is mapped to
        if srv_id is None:
            # No mapped node, assume the worst case node
            from Node import node_types
            wc_read= node_types[len(node_types) - 1][1]

            # input time is the size of the input divided by the worse case read time
            return output_size / wc_read
        else:
            from Node import Node
            mapped_node = Node.instance_map[srv_id]

            # Task has a mapped node, use it's actual speed data
            return output_size / mapped_node.write_speed


    # Data Transfer Time (dt) from task p to task j
    # @task_p(string) - the task's name
    # @task_j(string) -    "       "
    def dt(self, task_p, task_j, srv_id=None, hyp_node_type=None):
        # Note that it() defaults to zero if parent and task on the same node
        parent_srv_id = self.get_task_row(task_p)['service_instance_id']
        if srv_id == parent_srv_id and not (parent_srv_id is None):
            return 0
        return self.ot(task_p) + self.it(task_p, task_j, srv_id, hyp_node_type)


    # Minimuim possible runtime of a task on the best possible node
    def mrt(self, task):
        taskobj = self.get_task_row(task)

        from Node import node_types
        node_data = node_types[0]

        # Runtime is the task's runtime divided by the node's processing speed
        return taskobj.minimum_runtime / node_data[0]

    # Runtime of a task on a particular node type (service instance type)
    def rt(self, task, node_type):
        taskobj = self.get_task_row(task)
        try:
            from Node import node_types
            node_data = node_types[node_type]
            return taskobj.minimum_runtime / node_data[0]
        except Exception:
            raise Exception('invalid node type passed to rt()')

    def calc_pct(self, task_name):
        # calcualting pct per equation 8 of paper
        # calculating pct is required per line 9 of algorithm 1
        # pct is only calculated for the tasks for which all their predecessors have been mapped
        # task_name is a string which is used to query the pandas dataframe

        ST_task =  self.get_task_row(task_name)

        if len(ST_task.parents) != 0:
            max_pct = 0
            for p in ST_task.parents:
                ct = self.ct(p)
                dt = self.dt(task_name, p)
                max_pct = ct + dt if ct + dt > max_pct else max_pct

            max_pct = max_pct + self.mrt(task_name)
            pct = max_pct
        else:
            # Not using the it() function because I modified it to calculate based on a given parent and child task
            from Node import node_types
            wc_read = node_types[len(node_types) - 1][1]       #assume worst possible read speed
            read_time = sum([f['size'] for f in ST_task.files if f['link'] == 'input']) / wc_read
            pct = crt() + read_time
        self.update_task_field(task_name, 'predicated_completion_time', pct)
        return pct


    # "predicted cost"?
    def pc(self, task, node_type=None):
        from Node import node_types
        if node_type is None or node_type < 0 or node_type >= len(node_types):
            raise Exception('pc() called with invalid node type: ' + str(node_type))

        process_speed, read_speed, write_speed, price_per_hr = node_types[node_type]
        taskobj = self.get_task_row(task)

        # All in seconds
        read_time = sum([f['size'] for f in taskobj.files if f['link'] == 'input'])
        runtime = taskobj.minimum_runtime / process_speed
        write_time = sum([f['size'] for f in taskobj.files if f['link'] == 'output'])

        return (read_time + runtime + write_time) / 3600.0 * price_per_hr


    #Quick checksum to run at the end to ensure all tasks were processed
    def verify_workflow_completion(self):
        rdf = self.taskdf
        #if any tasks have not been completed yet, then the below relationship does not hold
        return (rdf[rdf.complete==True].complete.equals(rdf.complete))
