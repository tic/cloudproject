
KBs = lambda x : x * 1024.0
MBs = lambda x : x * KBs(1024)

from asyncio import sleep
from functools import reduce
import operator
class Task(object):
    def __init__(self, job_details, dupe=False):
        self.__done = False
        self.__ready = dupe
        self.__job = job_details
        self.__file_read_speed = MBs(110)
        self.__file_write_speed = MBs(75)

        # no speedup effect computed here. could add this in elsewhere if still wanted
        self.start_time = reduce(operator.add, map(lambda t: t['size'], filter(lambda file: file['link'] == 'input', job_details['files']))) / self.__file_read_speed

        # need to factor in parent tasks
        self.completion_time = job_details['runtime']

        self.data_time = reduce(operator.add, map(lambda t: t['size'], filter(lambda file: file['link'] == 'output', job_details['files']))) / self.__file_read_speed

    def duplicate(self):
        t = Task(self.__job, dupe=True)
        return t

    async def setup(self, speed):
        if self.__ready:
            return

        # sleep based on input file read time
        sleep_time = 0
        for file in self.__job['files']:
            if file['link'] == 'input':
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

    @staticmethod
    def computeLCT(task):
        # Compute LCT as done in the paper

        successor_tasks = [] # get the list of successor tasks, somehow
        if len(successor_tasks) == 0:
            return 0

        precursor_tasks = [] # get the list of parent tasks, somehow
        ct = 0
        for pt in precursor_tasks:
            pt_ct = pt.completion_time
            ct = pt_ct if pt_ct > ct else ct

        lct = ct
        for pt in precursor_tasks:
            dt = ct - pt.data_time
            lct = dt if dt < lct else lct

        return lct

    @staticmethod
    def computePCT(task):
        # Compute PCT as done in the paper

        precursor_tasks = []

        if len(precursor_tasks) == 0:
            pass
            # return crt + it(t_i^j)

        pct = 0
        for pt in precursor_tasks:
            candidate = pt.completion_time # + dt(e^i_(p, j))
            pct = candidate if candidate < pct else pct

        # pct += mrt(t^i_j)
