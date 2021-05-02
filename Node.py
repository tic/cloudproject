import asyncio

KBs = lambda x : x * 1024
MBs = lambda x : x * KBs(1024)

# Ordered from best to worst
# Format: (process speed, read speed, write speed, price)

# Node types are, in order:
# n4.2xlarge, n4.xlarge, n4.large, n4.small
# As loosely defined on p.139 of the paper.
# Processing speed is the number of CPUs times the base processing speed.
base_proc_speed = 10
base_io_speed = (MBs(100), MBs(75))
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
    def __init__(self, ntype=0):
        self.working = False
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
        self.provisioned_time = 0 # This is the amount of time the node is provisioned for - used in TC metric calculation
        self.execution_time = 0  



    async def run(self, task):
        # print(f'node {self.__id} working')
        await task.setup(self.speed)
        await task.run(self.speed)
        await task.finish(self.speed)
        self.working = False
        # print(f'node {self.__id} done!')

    def __str__(self):
        return f'node#{self.__id} w proc spd. {self.speed}x'

    def getID(self):
        return self.__id
