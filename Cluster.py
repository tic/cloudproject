from Node import Node
from Task import Tasks
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


# class SimpleTaskQueue(Tasks):

#     def get_next_task_name(self):
#         return self.taskdf[self.taskdf['complete'] == False].iloc[0]['name']





class Cluster(object):
    def __init__(self, nodes, process_speed=1):
        self.__nodes = list(map(lambda _ : Node(process_speed=process_speed), [0] * nodes))
        self.__queued_workflows = []
        self.__tasks = Tasks()
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
