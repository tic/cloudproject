
class Node(object):
    def __init__(self, process_speed=1):
        self.speed = process_speed
        self.__task = None

    def run(self, task, callback):
        self.__task = task
        task.setup()
        task.run()
        self.__task = None
        callback(self)

    def __str__(self):
        return f'node w proc spd. {self.speed}x'
