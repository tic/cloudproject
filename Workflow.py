
def generate_workflow(wf_name=None):
    from workflowhub.generator import MontageRecipe, SeismologyRecipe, EpigenomicsRecipe
    from random import randint
    if wf_name is None:
        wf_type = randint(0, 2)
    else:
        wf_dict = {"m":0, "s":1, "e":2}
        wf_type = wf_dict[wf_name] 
    template = [MontageRecipe, SeismologyRecipe, EpigenomicsRecipe][wf_type]
    recipe = template.from_num_tasks(num_tasks = randint(150, 400))

    wf_type = ['MontageRecipe', 'SeismologyRecipe', 'EpigenomicsRecipe'][wf_type]
    print(f'[WF] generating random {wf_type} workflow')

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

def gen_workflow_list(workflow_list=[]):
    wfs = []
    for wf in workflow_list:
        wfs.append(generate_workflow(wf))
    return wfs


class Workflow(object):
    def __init__(self, name, wf=None):
        if wf == None:
            wf = generate_workflow()

        self.wf = wf
        self.name = name
        from Task import crt
        self.arrival_time = crt()

    def __str__(self):
        return f'WF obj | {self.name}'

    # this function was never used
    def tasks(self):
        from Task import Tasks
        return list(map(lambda job : Task(job), self.wf['workflow']['jobs']))

    def get_task_json(self):
        return self.wf['workflow']['jobs']

    # Computes the completion time of a workflow of tasks
    # This was on our note sheet, but I'm not sure if we actually need it,
    # so it's a stub for now.
    def ct(self):
        pass
