import asyncio


class Node(object):
    id = 0
    def __init__(self, process_speed=1):
        self.speed = process_speed
        self.working = False
        self.__id = Node.id
        Node.id += 1

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
