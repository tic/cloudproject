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
        return list(filter(lambda node : not node.working, self.__nodes))

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

                # Algorithm 2: Task Scheduler
                self.task_schedule(t["name"])

                # for each mapped task, updated all child task unmapped_parent_count fields
                self.__tasks.signal_children_si_mapped(t["name"])


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
        Tasks = self.__tasks
        taskobj = Tasks.get_task_row(task)
        lct_tij = Tasks.lct(task)

        for service_instance in self.get_si_list(): # Pseudocode line 4
            temp_dt = [] # Pseudocode line 5

            # Pseudocode line 6
            ct_tij = Tasks.ct(task, service_instance.type)
            pc_tij = Tasks.pc(task, service_instance.type) # TODO: Tasks class does not have a pc method.

            while True: # Pseudocode line 7
                if ct_tij < min_completion_time: # Pseudocode line 8
                    # Pseudocode lines 9-10
                    selected_service_instance = service_instance
                    min_completion_time = max(ct_tij, lct_tij)
                    dup_tasks = list(temp_dt)

                    if ct_tij <= lct_tij and pc_tij < min_cost: # Pseudocode line 11
                        # Pseudocode line 12
                        min_cost = pc_tij
                        tag = True
                        break

                # Pseudocode line 13
                tb_min = float('inf')
                t_b = None
                for t_ip in taskobj.parents:
                    arg = Tasks.ct(t_ip) + Tasks.dt(t_ip, task)
                    if arg < tb_min:
                        tb_min = arg
                        t_b = t_ip

                # Pseudocode line 14
                WT_k = Tasks.taskdf[Tasks.taskdf.service_instance_id == service_instance.id]['name'] # this is supposed to get a list of the names of tasks which have been mapped to this service instance
                if t_b is not None and t_b not in temp_dt and t_b not in WT_k:
                    temp_dt.append(t_b) # Pseudocode line 15

                    # This is the amount of time the service instance will have to run the duplicated tasks
                    #   before it is able to run the actual task t_ij
                    pretask_duplication_overhead = 0
                    for t in temp_dt:
                        runtime = t.minimum_runtime / service_instance.process_speed
                        write_time = sum([f['size'] for f in t.files if f['link'] == 'output']) / service_instance.write_speed
                        pretask_duplication_overhead += runtime + write_time

                    # Update ct_tij by assuming that all the tasks in tempDT are duplicated to the current service instance
                    ct_tij += pretask_duplication_overhead

                    # Pseudocode lines 17-18
                    for t_k in WT_k:
                        if Tasks.ct(t_k) > Tasks.lct(t_k): break

                else: break # Pseudocode lines 19-20

        if tag == False: # Pseudocode line 21
            u_star = None # Pseudocode line 22

            # Pseudocode line 23
            from Node import node_types
            for u in range(len(node_types)):
                temp_dt = [] # Pseudocode line 24

                # Pseudocode line 25
                ct_tij = Tasks.ct(task, node_type=u)
                pc_tij = Tasks.pc(task, node_type=u)

                while True: # Pseudocode line 26
                    if ct_tij < min_cost: # Pseudocode line 27

                        # Pseudocode line 28
                        u_star = u
                        min_cost = max(ct_tij, lct_tij)
                        dup_tasks = list(temp_dt)

                        if ct_tij <= lct_tij and pc_tij < min_cost: # Pseudocode line 29

                            # Pseudocode line 30
                            min_cost = pc_tij
                            break

                    # Pseudocode line 31
                    t_b = None # TODO

                    if t_b is not None and t_b not in temp_dt: # Pseudocode line 32
                        temp_dt.append(t_b) # Pseudocode line 33

                        # Pseudocode line 34
                    else: break # Pseudocode lines 35-36

            if u_star is not None: # Pseudocode line 37
                pass
                # Pseudocode line 38
                # Lease a new service instance, SI_uk, with type u_star
                # selected_service_instance = SI_uk

                # Pseudocode line 39
                # Add SI_uk to siList (the list of service instances)
                # siList.append(SI_uk) # Our version of this isn't that simple

            # Pseudocode line 40
            # Map all the tasks in dup_tasks to selected_service_instance
            # Pseudocode line 41
            # Map argument "task" to selected_service_instance
            # argyemtn
