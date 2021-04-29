
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
            'parent_count',
            'unmapped_parent_count', # the number of parents who are not yet mapped to a service instance node. Used for Algorithm 1
            'complete',
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
                'parents': task['parents'],
                'children': task['children'],
                'parent_count': len(task['parents']),
                'unmapped_parent_count': len(task['parents']),
                'files': task['files'],
                'service_instance_id': None,
                'minimum_runtime': task['runtime'],
                'start_time': float('inf'),
                'complete': False,
            }, name=task['name']))


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
            rdf = rdf[rdf['service_instance_id'] == None]
        rdf = self.taskdf

        return rdf

    # returns a pd series representation of single task
    # @task(string) - the task's name
    def get_task_row(self, task):
        # might want to check to see if there are no duplicate names before squeezing
        return self.taskdf.loc[task].squeeze()

    def update_task_field(self, task, field, value):
        self.taskdf.loc[task, field] = value

    # unmaps tasks to service instances of all incomplete tasks
    # also resets the 'unmapped_parent_count' field to equal the parent count
    def unmap_service_instances(self):
        self.taskdf.loc[self.taskdf.complete == False, 'service_instance_id'] = None
        self.taskdf.loc[self.taskdf.complete == False, 'unmapped_parent_count'] = self.taskdf.loc[self.taskdf.complete == False, 'parent_count']

    # given a task name string, decrements the 'unmapped_parent_count' field for all children taskss
    # @task(string) - the task's name
    def signal_children_si_mapped(self, task):
        self.taskdf.loc[self.taskdf.parents.apply(lambda x: task in x), 'unmapped_parent_count']

    # Completion Time
    # @task(string) - the task's name
    def ct(self, task, node_type=None, duplicate=False):
        from Node import Node, node_types
        taskobj = self.get_task_row(task)
        if taskobj.service_instance_id is not None:
            node_type = Node.instance_map[taskobj.service_instance_id].type

        if node_type is None:
            node_type = len(node_types) - 1


        json_time = taskobj.minimum_runtime / node_types[node_type][0]

        task_parents = self.get_task_row(task).parents
        #TODO: possible performance issues due to recursive call to ct. Should we store this data in the dataframe?
        parent_max_ct = max([self.ct(t) for t in task_parents]) if task_parents else 0

        # If a task is being duplicated, the input time is voided
        if duplicate:
            return json_time + parent_max_ct

        it = self.it(task, node_type)
        return json_time + it + parent_max_ct


    def mitch_ct(self, task, curr_node=None, duplicated_tasks=None):
        from Node import Node, node_types
        taskobj = self.get_task_row(task)
        if taskobj.service_instance_id is not None:
            node_type = Node.instance_map[taskobj.service_instance_id].type

        # There's a specific node being looked at when calculating CT in most cases, hence the following
        if curr_node is None:
            if node_type is None:
                node_type = len(node_types) - 1
        else:
            node_type = Node.instance_map[curr_node].type

        json_time = taskobj.minimum_runtime / node_types[node_type][0]

        task_parents = self.get_task_row(task).parents
        parent_max_ct = max([self.get_task_row(t)['completion time'] for t in task_parents]) if task_parents else 0

        if not duplicated_tasks is None:
            curr_node_earliest_finish_time = self.taskdf[self.taskdf.service_instance_id == curr_node]['completion_time']
            curr_node_earliest_finish_time += sum([self.get_task_row(t).minimum_runtime / node_types[node_type][0] for t in duplicated_tasks])
            parent_max_ct = max(parent_max_ct, curr_node_earliest_finish_time)

        it = self.mitch_it(task, srv_id=curr_node, duplicated_tasks=duplicated_tasks)
        return json_time + it + parent_max_ct


    ## TODO: finish this function
    # @task(string) - the task's name
    def lct(self, task):

        taskobj = get_task_row(task)

        task_children = taskobj.children

        if task_children:
            return min(
                max(self.ct(p) for p in get_task_row(child).parents) + self.dt(task, child) for child in task_children
            )
        else:
            return 0

    # Input Time -- the time it takes a task to read in its files
    # @task(string) - the task's name
    def it(self, task, node_type=None):
        taskobj = self.get_task_row(task)
        srv_id = taskobj.service_instance_id # returns None if no service instance is found

        # Compute input size by summing the size of all inputs files
        input_size = sum([f['size'] for f in taskobj.files if f['link'] == 'input'])

        # Get the node the task is mapped to
        if srv_id is None:
            # No mapped node, assume the worst case node
            from Node import node_types
            wc_proc, wc_read, wc_write = node_types[len(node_types) - 1]

            # input time is the size of the input divided by the worse case read time
            return input_size / wc_read
        else:
            from Node import Node
            mapped_node = Node.instance_map[srv_id]

            # Task has a mapped node, use it's actual speed data
            return input_size / mapped_node.read_speed

    def get_parent_nodes(self, task):
        #return the unique parent nodes of the given task
        parent_nodes = [parent['service_instance_id'] for parent in self.get_task_row(task)['parents']]
        return list(set(parent_nodes)) #removes duplicate service_instance_id's


    def get_parents_on_same_node(self, task, node_id):
        #returns all the parents for a given task that are on the node being considered for teh current task
        parents = [parent for parent in self.get_task_row(task)['parents'] 
                    if self.get_task_row(parent)['service_instance_id']==node_id]
        return parents


    def get_parents_on_diff_node(self, task, node_id):
        #returns all the parents for a given task that are not on the node being considered for the current task
        parents = [parent for parent in self.get_task_row(task)['parents'] 
                    if self.get_task_row(parent)['service_instance_id']!=node_id]
        return parents


    def mitch_it(self, task, srv_id=None, duplicated_tasks=None):
        #assume worst case scenario - we don't know what service instance the task is considering
        if srv_id == None:
            return self.it(task)

        #Otherwise need to consider specific SI, such as in ct calculation in line 6 of TaskSchedule() pseudocode.
        # Compute input size by summing the size of all inputs files that come from parents on different service instances
        diff_SI_parents = self.get_parents_on_diff_node(task, srv_id)

        #if parent is duplicated then it's on the same node as the task at hand
        if not duplicated_tasks is None:
            diff_SI_parents = [t for t in diff_SI_parents if not t in duplicated_tasks]
         
        output_from_diff_SI = [f for parent in diff_SI_parents for f in self.get_task_row(parent)['files'] if f['name']=='output']
        input_size = sum([f['size'] for f in taskobj.files if f['link'] == 'input'] and f['name'] in output_from_diff_SI)
        
        from Node import Node
        candidate_node = Node.instance_map[srv_id]

        # what's the read speed for the task assuming the passed in node is used
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
            wc_proc, wc_read, wc_write = node_types[len(node_types) - 1]

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
    def dt(self, task_p, task_j):
        # if tasks  and j are on the same service instance,
        # the data transfer time is zero
        task_p_obj = self.get_task_row(task_p)
        task_j_obj = self.get_task_row(task_j)
        if task_p_obj.service_instance_id == task_j_obj.service_instance_id:
            return 0

        return self.ot(task_p) + self.it(task_j)

    # Minimuim possible runtime of a task on the best possible node
    def mrt(self, task):
        taskobj = self.get_task_row(task)

        from Node import node_types
        best_proc_speed, rs, ws = node_types[0]

        # Runtime is the task's runtime divided by the node's processing speed
        return taskobj.minimum_runtime / best_proc_speed

    # Runtime of a task on a particular node type (service instance type)
    def rt(self, task, node_type):
        taskobj = self.get_task_row(task)
        try:
            from Node import node_types
            proc_speed, rs, ws = node_types[node_type]
            return taskobj.minimum_runtime / proc_speed
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
<<<<<<< HEAD
            for p in self.taskdf['parents']:
                ct = self.taskdf[self.taskdf['parents'] == p]['completion_time'] #I feel like completion time gets calculated in the task_schedule function
                dt = self.dt(p, task_name)
=======
            for p in ST_task.parents:
                #ct = self.get_task_row(p)['completion_time'] #I feel like completion time gets calculated in the task_schedule function
                ct = self.ct(p)
                dt = self.dt(task_name, p)
>>>>>>> 1e193f358ec6dc083ec11a718256e5c5212753e8
                max_pct = ct + dt if ct + dt > max_pct else max_pct
            max_pct = max_pct + self.mrt(task_name)
            pct = max_pct
        else:
            pct = crt() + self.it(task_name)
        self.update_task_field(task_name, 'predicated_completion_time', pct)
        return pct


    def calc_ct(self, task_name):
        # dynamically calculate the completion_time of a given task
        # task_name is a string that can be queried in dataframe
        return ct
