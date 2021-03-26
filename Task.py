
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

    def duplicate(self):
        t = Task(self.__job, dupe=True)
        return t

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

    async def finish(self, speed):
        # sleep based on output file write time
        sleep_time = 0
        for file in self.__job['files']:
            if file['link'] == 'output':
                sleep_time += file['size']

        sleep_time /= self.__file_write_speed
        await sleep(sleep_time / speed)
