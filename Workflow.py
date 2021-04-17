def generate_workflow():
    from workflowhub.generator import MontageRecipe, SeismologyRecipe, EpigenomicsRecipe
    from random import randint
    template = [MontageRecipe, SeismologyRecipe, EpigenomicsRecipe][randint(0, 2)]
    recipe = template.from_num_tasks(num_tasks = randint(150, 400))

    from workflowhub import WorkflowGenerator
    generator = WorkflowGenerator(recipe)

    wfs = generator.build_workflows(num_workflows=1)
    for i, wf in enumerate(wfs):
        fname = f'./wf_{i}.json'
        wf.write_json(fname)
        with open(fname, 'r') as file:
            from json import loads
            x = loads(file.read())
            wfs[i] = x
        from os import remove
        #remove(fname)

    return wfs[0]



class Workflow(object):
    def __init__(self, name, wf=None):
        if wf == None:
            wf = generate_workflow()

        self.wf = wf
        self.name = name

    def __str__(self):
        return f'WF obj | {self.name}'

    def get_task_json(self):
        return self.wf['workflow']['jobs']

    def tasks(self):
        from Task import Task
        x = list(map(lambda job : Task(job), self.wf['workflow']['jobs']))

        return x
