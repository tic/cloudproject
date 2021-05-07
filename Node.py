import asyncio

from Task import crt

KBs = lambda x : x * 1024
MBs = lambda x : x * KBs(1024)

# Ordered from best to worst
# Format: (process speed, read speed, write speed, price)

# Node types are, in order:
# n4.2xlarge, n4.xlarge, n4.large, n4.small
# As loosely defined on p.139 of the paper.
# Processing speed is the number of CPUs times the base processing speed.
# CCR is the ratio between communication and compute time
CCR = 2
base_proc_speed = 10
base_io_speed = (MBs(base_proc_speed*CCR), MBs(base_proc_speed*CCR)) #(MBs(100), MBs(75))
node_types = [
    (base_proc_speed * 8, *[1.9*x for x in base_io_speed], 0.336),
    (base_proc_speed * 4, *[1.5*x for x in base_io_speed], 0.168),
    (base_proc_speed * 2, *[1.2*x for x in base_io_speed], 0.047),
    (base_proc_speed * 1, *[1.0*x for x in base_io_speed], 0.023),
]

class Node(object):
    id = 0
    instance_map = {}

    # @type - integer in [ 0, len(node_types) )
    def __init__(self, task_manager, ntype=0):
        self.operating = True
        self.task_manager = task_manager
        self.__id = Node.id
        Node.id += 1
        Node.instance_map[self.__id] = self

        # Add properties for the various node speeds
        if ntype < 0 or ntype > len(node_types) - 1:
            ntype = 0
        node_data = node_types[ntype]
        self.ntype = ntype
        self.process_speed = node_data[0]
        self.read_speed = node_data[1]
        self.write_speed = node_data[2]
        self.cost = node_data[3]

        self.awaken_time = crt()
        self.provisioned_time = 0 # This is the amount of time the node is provisioned *and awake* for - used in TC metric calculation
        self.execution_time = 0

    # Written in a way to avoid using braches to ensure maximum performance
    def get_live_provisioned_time(self):
        return int(self.sleeping) * self.provisioned_time + int(not self.sleeping) * (self.provisioned_time + crt() - self.awaken_time)

    # Code used to manage provisioned time is written without using branches
    #  to ensure maximum possible performance.
    async def node_event_loop(self):
        self.sleeping = False
        task_search_attempts = 1
        while self.operating:
            #print("operating")
            #next_task = self.task_manager.get_next_task(self.__id)
            next_task = self.task_manager.get_next_task_fifo(self.__id)
            #print(next_task)
            #print("next task is ", str(next_task))
            if next_task is not None:
                #print("next task is not none")
                # ###### Provisioned time management ###### #
                condition = (task_search_attempts > 0)
                self.awaken_time = crt() * condition + int(not condition) * self.awaken_time
                task_search_attempts = 0
                #print("trying to awaken")
                # ###### #

                # Simulate the task
                # Total execution time is the input time, output time, and run time
                #task_execution_time = self.task_manager.it(next_task) + self.task_manager.ot(next_task) + self.task_manager.rt(next_task, self.ntype)
                #total_read_time = self.task_manager.it()
                #total_write_time = self.task_manager.ot(next_task)
                curr_time = crt()
                # wait for all predecessors to be met before running
                #print("right before printing node id")
                #print("node id ", self.__id, " running ", next_task.name)
                await self.task_manager.wait_to_run(next_task, self.__id)
                #print('successfully waited') 
                total_run_time = self.task_manager.rt(next_task, self.ntype)
                task_execution_time = total_run_time
                print(f'node#{self.__id} running task {next_task} ({task_execution_time}s)')
                await asyncio.sleep(task_execution_time)

                # Update task completion time
                self.task_manager.update_task_field(next_task, 'completion_time', crt())
                self.task_manager.update_task_field(next_task, 'complete', True)

                # Update node execution time metric
                self.execution_time += task_execution_time
                print(f'node#{self.__id} finished {next_task}')
            else:
                # Node has not been assigned a task.
                # Allow other things to run

                # ###### Provisioned time management ###### #
                next_sleeping = not bool(task_search_attempts - 5) # node goes to sleep after 5 consecutive checks for tasks
                self.provisioned_time += int(not self.sleeping) * int(next_sleeping) * (crt() - self.awaken_time)
                self.sleeping = next_sleeping
                task_search_attempts = (task_search_attempts + 1) * int(not self.sleeping) + task_search_attempts * int(self.sleeping)
                # ###### #

                # If node is self.sleeping, sleep for 1 second. If awake, sleep for just 0.2s
                await asyncio.sleep(2 * int(self.sleeping) + 0.2 * int(not self.sleeping))

    def __str__(self):
        return f'node#{self.__id} w proc spd. {self.speed}x'

    def getID(self):
        return self.__id
