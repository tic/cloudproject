from Node import Node
import asyncio
import time
from collections import OrderedDict


class TaskQueue(object):
    def __init__(self):
        self.__task_ordered_dict = OrderedDict()

    @property
    def length(self):
        return len(self.__task_ordered_dict)

    def add_task(self, task):
        self.__task_ordered_dict[task.name] = {
            'task': task,
        }

    def add_tasks(self, tasks):
        for t in tasks:
            self.add_task(t)

    def get_tasks(self):
        return self.__task_ordered_dict
    
    def pop_task(self):
        k, v = self.__task_ordered_dict.popitem(last=False)
        return v['task']

import pandas
import json

KBs = lambda x : x * 1024
MBs = lambda x : x * KBs(1024)

READ_SPEED = MBs(500)
WRITE_SPEED = MBs(370)

class Tasks(object):

    def __init__(self):
        self.taskdf = pandas.DataFrame(columns=[
            'name',
            'workflow',
            'parents',
            'children',
            'files',
            'service_instance_id', # the service instance id that this task will be run on
            'completion_time', # calculated an used in Algorithm 2
            'predicated_completion_time', # used in Algorithm 1 to sort the tasks_st
            'latest_completion_time', # Used in Algorithm 2. Calcualted by using equation 8
            'complete',
        ])


    def add_tasks_from_wf(self, wf):

        # cancel mappings for waiting tasks in queue
        # lines 4-5 of Algorithm 1
        self.taskdf[self.taskdf['complete'] == False]['service_instance_id'] = None
        self.taskdf[self.taskdf['complete'] == False]['predicated_completion_time'] = None
        self.taskdf[self.taskdf['complete'] == False]['latest_completion_time'] = None



        wf_name = wf.name
        for task in wf.get_task_json():
            self.taskdf = self.taskdf.append({
                'name': task['name'],
                'workflow': wf_name,
                'parents': task['parents'],
                'children': task['children'],
                'files': task['files'],
                'service_instance_id': None,
                'complete': False,
            }, ignore_index=True)

        # get all tasks 

        # find all tasks with incomplete and unassigned parents
        # * find all tasks that are complete == False and service_instance_id == None, assign this list as "incomplete_parents"
        # explode self.taskdf. find all tasks within this df that have a parent in the "incomplete_parents" list
        # tasks that are not within the above list are tasks that "have not precursor or all their precursors have ben mapped"
        # Algorithm 1, line 8

        incomplete_parent_names = set(self.taskdf[self.taskdf['complete'] == False & self.taskdf['service_instance_id'].isnull()]['name'].tolist())
        taskdf_exploded = self.taskdf.explode('parents')
        tasks_with_incomplete_parents = set(taskdf_exploded[taskdf_exploded['parents'].isin(incomplete_parent_names)]['name'].tolist())
        tasks_st = self.taskdf[(~self.taskdf['name'].isin(tasks_with_incomplete_parents)) & (self.taskdf['complete'] == False) & (self.taskdf['service_instance_id'].isnull())]

        # TODO: sort all tasks in tasks_st in non_ascending order based on pct

        def predict_completion_time(t):
            if t.parents:
                # TODO
                return time.time()
            else:
                # equation 8. Current runtime (crt) + time for task t to read-in the data (it)
                return time.time() + sum([f['size'] for f in t.files if f['link'] == 'input']) / READ_SPEED

        tasks_st['predicated_completion_time'] = tasks_st.apply(lambda x: predict_completion_time(x), axis=1)

        tasks_st.sort_values(by='predicated_completion_time', ascending=False)


        def latest_completion_time(t):
            if t.children:
                # TODO
                pass
            else:
                return 0


        siList = []
        siTypeList = []

        for index, row in self.taskdf.iterrows():
            print (row)



# class SimpleTaskQueue(Tasks):

#     def get_next_task_name(self):
#         return self.taskdf[self.taskdf['complete'] == False].iloc[0]['name']




class RTSATD(Tasks):

    def add_tasks_from_wf(self, wf):

        super().add_tasks_from_wf(wf)





class Cluster(object):
    def __init__(self, nodes, process_speed=1):
        self.__nodes = list(map(lambda _ : Node(process_speed=process_speed), [0] * nodes))
        self.__queued_workflows = []
        self.__tasks = RTSATD()
        self.__task_queue = ()
        self.working = False

    async def submit_workflow(self, wf):
        from Workflow import Workflow
        assert(type(wf) == Workflow)
        self.__tasks.add_tasks_from_wf(wf)


        self.__queued_workflows.append(wf)
        await self.__start_workflow()

    async def __start_workflow(self):
        if len(self.__queued_workflows) == 0:
            self.working = False
            print('all workflows completed!')
            return

        # otherwise, translate a workflow into tasks
        next_wf = self.__queued_workflows.pop(0)

        for t in next_wf.tasks():
            print (t.name)

        self.__task_queue.add_tasks(next_wf.tasks())

        import pdb
        pdb.set_trace()

        # node performance tracking
        node_job_count = [0] * len(self.__nodes)

        print(f'starting workflow: \'{next_wf.name}\'')
        while self.__task_queue.length > 0:
            for node in self.__nodes:
                if not node.working and self.__task_queue.length > 0:
                    node_job_count[node.getID()] += 1
                    node.working = True
                    asyncio.create_task(node.run(self.__task_queue.pop_task()))
                    print(f'assigned node {node.getID()} task ({self.__task_queue.length} remaining)')
            await asyncio.sleep(0.01)
        done = False
        while not done:
            done = True
            for node in self.__nodes:
                if node.working:
                    done = False
                    break
            await asyncio.sleep(1)

        print('workflow done!')
        for node, count in enumerate(node_job_count):
            print(f'node {node} processed {count} tasks')
