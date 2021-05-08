from Node import Node
from Task import Tasks, crt
import asyncio
from Metrics import ANCT, TC, RU

class Cluster(object):
    def __init__(self, scheduler_type=None, node_list = []):
        self.__nodes = []
        self.__node_event_loops = []
        self.__queued_workflows = []
        self.__tasks = Tasks()
        self.__task_queue = []
        self.working = False
        self.wf_list = []   #list of all workflows that arrive
        self.scheduler_type = scheduler_type
        self.node_list = node_list


    def set_up_nodes_for_fifo(self, scheduler_type, node_list):
        if scheduler_type == "fifo":  
            self.scheduler_type = scheduler_type
        #node_list should be in the same order as types in node_types
            for i in range(len(node_list)):
                number_of_nodes_of_type_i = node_list[i]
                for n in range(number_of_nodes_of_type_i):
                    SI_uk = Node(self.__tasks, i)
                    nev = asyncio.create_task(SI_uk.node_event_loop())
                    self.__node_event_loops.append(nev)
                    selected_service_instance = SI_uk
                    self.__nodes.append(selected_service_instance)

    # Get available service instances
    def get_si_list(self):
        return self.__nodes #This makes more sense w/in context of TaskSchedule algorithm

    async def event_loop(self):
        sock = None
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('127.0.0.1', 15555))
            sock.listen(1)
            sock.setblocking(False)

            loop = asyncio.get_event_loop()
            self.set_up_nodes_for_fifo(self.scheduler_type, self.node_list)
            print('cluster is ready')
            while True: # while workflows arrive...
                # Receive a workflow from a sender
                client, _ = await loop.sock_accept(sock)
                data = (await loop.sock_recv(client, 512)).decode('utf-8')
                if '\x01' in data:
                    # Special message to trigger task completion sanity check
                    complete = self.__tasks.verify_workflow_completion()
                    if complete:
                        anct = ANCT(self.__tasks, self.wf_list)
                        tc = TC(self.__nodes)
                        ru = RU(self.__nodes)
                        print("All tasks completed")
                        print("ANCT is: ", anct)
                        print("TC is: ", tc)
                        print("ru is: ", ru)
                    else:
                        print("Not all tasks completed")
                    continue
                while '\x00' not in data:
                    block = (await loop.sock_recv(client, 512)).decode('utf-8')
                    data += block

                print('workflow received')
                # Parse the data as a json, then as a workflow
                import json
                from Workflow import Workflow
                try:
                    n = 10
                    import random, string
                    random_wf_name = ''.join([random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(n)] + ['.wf'])
                    wf = Workflow(random_wf_name, json.loads(data[:-1]))
                except Exception:
                    print('received malformed json')
                    continue
                self.wf_list.append(wf)

                if not self.scheduler_type is None:
                    self.__tasks.add_tasks_from_wf(wf)
                    print("Tasks added to Dataframe")
                    await asyncio.sleep(1)
                    
                if self.scheduler_type is None:
                    # Workflow received. Proceed with the algorithm!
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
                        task_pct = sorted(task_pct, key=lambda x: x['pct'], reverse=True)
                        ntasks = len(task_pct) - 1
                        for i, t in enumerate(task_pct):
                            # Algorithm 2: Task Scheduler
                            self.task_schedule(t["name"])

                            # for each mapped task, updated all child task unmapped_parent_count fields
                            self.__tasks.signal_children_si_mapped(t["name"])
                    print("tasks all scheduled")
                    # Tasks scheduled. Release control for a bit
                    await asyncio.sleep(1)

        except Exception as err:
            print(err)
        finally:
            sock.close()

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
        #print("lct is ", lct_tij)

        for service_instance in self.get_si_list(): # Pseudocode line 4
            temp_dt = [] # Pseudocode line 5

            # Pseudocode line 6
            ct_tij = Tasks.ct(task, service_instance.getID())
            pc_tij = Tasks.pc(task, service_instance.ntype)

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
                t_b_time, t_b = Tasks.get_earliest_start_time(task, srv_id=service_instance.getID())

                # Pseudocode line 14
                WT_k = Tasks.taskdf[Tasks.taskdf.service_instance_id == service_instance.id].index # this is supposed to get a list of the names of tasks which have been mapped to this service instance
                if t_b is not None and t_b not in temp_dt and t_b not in WT_k:
                    temp_dt.append(t_b) # Pseudocode line 15

                    # This is the amount of time the service instance will have to run the duplicated tasks
                    #   before it is able to run the actual task t_ij
                    ct_tij = Tasks.ct(task, curr_node=service_instance.getID(), duplicated_tasks=temp_dt)

                    # Pseudocode lines 17-18
                    for t_k in WT_k:
                        if t_k.completion_time > Tasks.lct(t_k): break

                else: break # Pseudocode lines 19-20


        if tag == False: # Pseudocode line 21
            u_star = None # Pseudocode line 22

            # Pseudocode line 23
            from Node import node_types
            for u in range(len(node_types)):
                temp_dt = [] # Pseudocode line 24

                # Pseudocode line 25
                ct_tij = Tasks.ct(task, hyp_node_type=u)
                pc_tij = Tasks.pc(task, node_type=u)

                while True: # Pseudocode line 26
                    
                    if ct_tij < min_completion_time:

                        # Pseudocode line 28
                        u_star = u
                        min_completion_time = max(ct_tij, lct_tij)
                        dup_tasks = list(temp_dt)

                        if ct_tij <= lct_tij and pc_tij < min_cost: # Pseudocode line 29

                            # Pseudocode line 30
                            min_cost = pc_tij
                            break

                    # Pseudocode line 31
                    t_b_time, t_b = Tasks.get_earliest_start_time(task, hyp_node_type=u)
                    if t_b is not None and t_b not in temp_dt: # Pseudocode line 32
                        temp_dt.append(t_b) # Pseudocode line 33

                        # Pseudocode line 34
                        # This is the amount of time the service instance will have to run the duplicated tasks
                        #   before it is able to run the actual task t_ij
                        # Update ct_tij by assuming that all the tasks in tempDT are duplicated to the current service instance
                        ct_tij = Tasks.ct(task, duplicated_tasks=temp_dt, hyp_node_type=u)

                    else: break # Pseudocode lines 35-36

            if u_star is not None: # Pseudocode line 37
                # Pseudocode line 38
                # Lease a new service instance, SI_uk, with type u_star

                SI_uk = Node(self.__tasks, u_star)
                nev = asyncio.create_task(SI_uk.node_event_loop())
                self.__node_event_loops.append(nev)
                selected_service_instance = SI_uk

                # Pseudocode line 39
                # Add SI_uk to siList (the list of service instances)
                self.__nodes.append(selected_service_instance)

        # Pseudocode line 40
        # Map all the tasks in dup_tasks to selected_service_instance
        dup_task_new_names = [Tasks.duplicate_task(d) for d in dup_tasks]
        Tasks.update_task_field(dup_task_new_names, 'service_instance_id', selected_service_instance.getID())
        Tasks.update_task_field(dup_task_new_names, 'scheduled', crt())
        # Pseudocode line 41
        # Map argument "task" to selected_service_instance
        print(f'mapping {task} to {selected_service_instance.getID()}')
        Tasks.update_task_field(task, 'scheduled', crt())
        Tasks.update_task_field(task, 'service_instance_id', selected_service_instance.getID())
        Tasks.update_task_field(task, 'completion_time', min_completion_time)
