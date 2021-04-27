from Node import Node
from Task import Tasks
import asyncio
from time import sleep

class Cluster(object):
    def __init__(self, nodes, process_speed=1):
        self.__nodes = list(map(lambda _ : Node(process_speed=process_speed), [0] * nodes))
        self.__queued_workflows = []
        self.__tasks = Tasks()
        self.__task_queue = []
        self.working = False

    # Get available service instances
    def get_si_list(self):
        return list(filter(lambda node : not node.working, self._nodes))

    async def submit_workflow(self, wf):
        from Workflow import Workflow
        assert(type(wf) == Workflow)

        ###
        # Algorithm 1
        ###
        self.__tasks.unmap_service_instances() # cancel all service instance mappings

        # might need a function to cancel rent plans here

        self.__tasks.add_tasks_from_wf(wf) # tasks from workflow added to task instance

        while self.__tasks.get_tasks(mapped=False).size > 0:
            unmapped_tasks = self.__tasks.get_tasks(mapped=False)
            task_names = unmapped_tasks[unmapped_tasks['unmapped_parent_count'] == 0].index.tolist()
            task_pct = [{"name": n, "pct": self.__tasks.calc_pct(n)} for n in task_names]
            task_pct = sorted(x, key=lambda x: x['pct'], reverse=True)

            for t in task_pct:
                print (t["name"])
                ##
                # Add algorithm 2 Task Scheduler here

                # for each mapped task, updated all child task unmapped_parent_count fields

        await self.__start_workflow()

    async def __start_workflow(self):
        if len(self.__queued_workflows) == 0:
            self.working = False
            print('all workflows completed!')
            return

        # otherwise, translate a workflow into tasks
        next_wf = self.__queued_workflows.pop(0)
        self.__task_queue = next_wf.tasks()

        # node performance tracking
        node_job_count = [0] * len(self.__nodes)

        print(f'starting workflow: \'{next_wf.name}\'')
        while len(self.__task_queue) > 0:
            for node in self.__nodes:
                if not node.working and len(self.__task_queue) > 0:
                    node_job_count[node.getID()] += 1
                    node.working = True
                    asyncio.create_task(node.run(self.__task_queue.pop(0)))
                    print(f'assigned node {node.getID()} task ({len(self.__task_queue)} remaining)')
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

    def task_schedule(self, task):
        # Variable setup (pseudocode lines 1-2)
        selected_service_instance = None
        tag = False
        dup_tasks = []
        min_completion_time = float('inf')
        min_cost = float('inf')

        # Pseudocode line 3
        # self.__tasks
