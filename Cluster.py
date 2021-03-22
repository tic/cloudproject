from Node import Node

class Cluster(object):
    def __init__(self, nodes):
        self.__nodes = list(map(lambda _ : Node(), [0] * nodes))
        self.__queued_workflows = []
        self.__task_queue = []

    def submit_workflow(wf):
        from Workflow import Workflow
        assert(type(wf) == Workflow)
        self.__queued_workflows.append(wf)
        if len(self.__task_queue) == 0:
            self.__start_workflow()

    def __start_workflow(self):
        if len(self.__queued_workflows) == 0:
            print('all workflows completed!')
            return

        # otherwise, translate a workflow into tasks
        next_wf = self.__queued_workflows.pop(0)
        print(f'starting workflow: \'{wf.name}\'')

    def task_done(self, node):
        if len(self.__task_queue) == 0:
            print('workflow done')
            self.__start_workflow()
