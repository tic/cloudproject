
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

        # cancel mappings for waiting tasks in queue
        # lines 4-5 of Algorithm 1
        self.taskdf[self.taskdf['complete'] == False]['service_instance_id'] = None
        self.taskdf[self.taskdf['complete'] == False]['predicated_completion_time'] = None
        self.taskdf[self.taskdf['complete'] == False]['latest_completion_time'] = None



        wf_name = wf.name
        for task in wf.get_task_json():
            self.taskdf = self.taskdf.append({
                'name': task['name'],
                'workflow': wf_name,
                'parents': task['parents'],
                'children': task['children'],
                'files': task['files'],
                'service_instance_id': None,
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
                } for c in task['parents']
            ])

            # create edges to connect child tasks
            self.edgedf = self.edgedf.append([
                {
                    'tasks': ','.join([task['name'], c]),
                    'prevtask': task['name'],
                    'nexttask': c
                } for c in task['children']
            ])

        # get all tasks 

        # find all tasks with incomplete and unassigned parents
        # * find all tasks that are complete == False and service_instance_id == None, assign this list as "incomplete_parents"
        # explode self.taskdf. find all tasks within this df that have a parent in the "incomplete_parents" list
        # tasks that are not within the above list are tasks that "have not precursor or all their precursors have ben mapped"
        # Algorithm 1, line 8

        # incomplete_parent_names = set(self.taskdf[self.taskdf['complete'] == False & self.taskdf['service_instance_id'].isnull()]['name'].tolist())
        # taskdf_exploded = self.taskdf.explode('parents')
        # tasks_with_incomplete_parents = set(taskdf_exploded[taskdf_exploded['parents'].isin(incomplete_parent_names)]['name'].tolist())
        # tasks_st = self.taskdf[(~self.taskdf['name'].isin(tasks_with_incomplete_parents)) & (self.taskdf['complete'] == False) & (self.taskdf['service_instance_id'].isnull())]

        # # TODO: sort all tasks in tasks_st in non_ascending order based on pct

        # def predict_completion_time(t):
        #     if t.parents:
        #         # TODO
        #         return time.time()
        #     else:
        #         # equation 8. Current runtime (crt) + time for task t to read-in the data (it)
        #         return time.time() + sum([f['size'] for f in t.files if f['link'] == 'input']) / READ_SPEED

        # tasks_st['predicated_completion_time'] = tasks_st.apply(lambda x: predict_completion_time(x), axis=1)

        # tasks_st.sort_values(by='predicated_completion_time', ascending=False)


        # def latest_completion_time(t):
        #     if t.children:
        #         # TODO
        #         pass
        #     else:
        #         return 0


        # siList = []
        # siTypeList = []

        # for index, row in self.taskdf.iterrows():
        #     print (row)