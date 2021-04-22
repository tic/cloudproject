
from asyncio import sleep
import pandas
import json

KBs = lambda x : x * 1024
MBs = lambda x : x * KBs(1024)

READ_SPEED = MBs(500)
WRITE_SPEED = MBs(370)

class Task(object):
    
    def __init__(self, job_details, dupe=False):
        self.__done = False
        self.__ready = dupe
        self.__name = job_details["name"]
        self.__job = job_details
        self.__file_read_speed = MBs(500)
        self.__file_write_speed = MBs(370)

    @property
    def name(self):
        return self.__name

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

class WorkingTask(Task):
    def __init__(self, *args, **kwargs):
        pass


class Tasks(object):

    def __init__(self):
        self.taskdf = pandas.DataFrame(columns=[
            'name',
            'workflow',
            'parents',
            'children',
            'files',
            'service_instance_id', # the service instance id that this task will be run on
            'minimum_runtime',
            'start_time', # start time of the task
            'completion_time', # calculated an used in Algorithm 2
            'predicated_completion_time', # used in Algorithm 1 to sort the tasks_st
            'latest_completion_time', # Used in Algorithm 2. Calcualted by using equation 8
            'complete',
        ])

        self.edgedf = pandas.DataFrame(columns=[
            'tasks', # should be in a standardized format like <NAME>,<NAME>
            'prevtask',
            'nexttask',
        ])

    def add_tasks_from_wf(self, wf):

        wf_name = wf.name
        for task in wf.get_task_json():
            self.taskdf = self.taskdf.append({
                'name': task['name'],
                'workflow': wf_name,
                'parents': task['parents'],
                'children': task['children'],
                'files': task['files'],
                'service_instance_id': None,
                'minimum_runtime': task['runtime'],
                'start_time': float(‘inf’),
                'complete': False,
            }, ignore_index=True)


            # the next two methods may cause issues: there is no check in place to determin if the child task exists
            # we're relying on the imported workframe data to be pristine

            # create edges to connect parent tasks
            self.edgedf = self.edgedf.append([
                {
                    'tasks': ','.join([p, task['name']]),
                    'prevtask': task['name'],
                    'nexttask': p
                } for p in task['parents']
            ])

            # create edges to connect child tasks
            self.edgedf = self.edgedf.append([
                {
                    'tasks': ','.join([task['name'], c]),
                    'prevtask': task['name'],
                    'nexttask': c
                } for c in task['children']
            ])