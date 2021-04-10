
KBs = lambda x : x * 1024
MBs = lambda x : x * KBs(1024)

from asyncio import sleep
class Task(object):
    def __init__(self, job_details, dupe=False):
        self.__done = False
        self.__ready = dupe
        self.__job = job_details
        self.__file_read_speed = MBs(500)
        self.__file_write_speed = MBs(370)
        self.__workflow = None
        self.__parents = job_details["parents"]
        self.__children = job_details["children"]

    def duplicate(self):
        t = Task(self.__job, dupe=True)
        return t
    
    def get_parents(self):
        return self.__parents

    def get_name(self):
        return self.__job["name"]

    def remove_parent(self, parent):
        self.__parents.remove(parent)
        return

    async def setup(self, speed):
        if self.__ready:
            return

        # sleep based on input file read time
        sleep_time = 0
        for file in self.__job['files']:
            if file['link'] == 'output':
                sleep_time += file['size']
        sleep_time /= self.__file_read_speed
        await sleep(sleep_time / speed)
        self.__ready = True

    async def run(self, speed):
        # sleep based on job runtime
        run_time = self.__job['runtime'] / speed
        await sleep(run_time)

    async def finish(self, speed, task, task_queue):
        # sleep based on output file write time
        sleep_time = 0
        for file in self.__job['files']:
            if file['link'] == 'output':
                sleep_time += file['size']

        sleep_time /= self.__file_write_speed
        await sleep(sleep_time / speed)

        #removing task from parent attribute in its children
        for child in self.__children:
            for item in task_queue:
                if item.get_name() == child:
                    item.remove_parent(task.get_name())