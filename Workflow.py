from workflowhub.generator import MontageRecipe, SeismologyRecipe, EpigenomicsRecipe

def generate_workflow():
    from random import randint
    template = [MontageRecipe, SeismologyRecipe, EpigenomicsRecipe][randint(0, 2)]
    recipe = template.from_num_tasks(num_tasks = randint(100, 400))

    from workflowhub import WorkflowGenerator
    generator = WorkflowGenerator(recipe)

    wfs = generator.build_workflows(num_workflows=1)
    for i, wf in enumerate(wfs):
        fname = f'./wf_{i}.json'
        wf.write_json(fname)
        with open(fname, 'r') as file:
            from json import loads
            wfs[i] = loads(file.read())
        from os import remove
        remove(fname)

    return wfs[0]

class Workflow(object):
    def __init__(self, name, wf=None):
        if wf == None:
            wf = generate_workflow()

        self.wf = wf
        self.name = name

    def __str__(self):
        return f'WF obj | {self.name}'

wf = Workflow('test')
