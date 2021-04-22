import asyncio

KBs = lambda x : x * 1024
MBs = lambda x : x * KBs(1024)

# Ordered from best to worst
# Format: (process speed, read speed, write speed)
node_types = [
    (50, MBs(200), MBs(150)),
    (15, MBs(150), MBs(110)),
    (3, MBs(100), MBs(70)),
    (1, MBs(50), MBs(30))
]

class Node(object):
    id = 0
    instance_map = {}
    def __init__(self, process_speed=1):
        self.speed = process_speed
        self.working = False
        self.__id = Node.id
        Node.id += 1
        Node.instance_map[self.__id] = self

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
