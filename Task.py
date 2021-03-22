
class Task(object):
    def __init__(self, job_details, dupe=False):
        self.__done = False
        self.__ready = dupe
        self.__job = job_details

    def duplicate(self):
        t = Task(self.__job, dupe=True)
        return t

    def setup(self):
        if self.__ready:
            return

        # sleep based on job information
        self.__ready = True

    def run(self):
        # sleep based on job information
        pass
