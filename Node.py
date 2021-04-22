import asyncio


class Node(object):
    id = 0

    # Machine types define process, read, and write speeds
    # The overall processing multiplier of the node is factored in later
    DedicatedMachine = (100, 100, 100)  # Type 0
    DedicatedCore = (15, 15, 15)        # Type 1
    DedicatedContainer = (3, 3, 3)      # Type 2
    SharedContainer = (1, 1, 1)         # Type 3

    def __init__(self, type, process_speed=1):
        self.__id = Node.id
        Node.id += 1

        self.working = False

        self.type = type
        if type == 0: self.proc_speed, self.read_speed, self.write_speeed = Node.DedicatedMachine
        elif type == 1: self.proc_speed, self.read_speed, self.write_speeed = Node.DedicatedCore
        elif type == 2: self.proc_speed, self.read_speed, self.write_speeed = Node.DedicatedContainer
        else: self.proc_speed, self.read_speed, self.write_speeed = Node.SharedContainer

    async def run(self, task):
        # print(f'node {self.__id} working')
        self.task = task
        await task.setup(self.proc_speed)
        await task.run(self.proc_speed)
        await task.finish(self.proc_speed)
        self.task = None
        self.working = False
        # print(f'node {self.__id} done!')

    def __str__(self):
        return f'node#{self.__id} w proc spd. {self.speed}x'

    def getID(self):
        return self.__id
