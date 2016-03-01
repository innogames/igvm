from glob import glob
import os.path

def load_hooks():
    hooks = glob(os.path.join(os.path.dirname(__file__), '*.py'))
    for hook in hooks:
        if hook == '__init__.py':
            continue
        execfile(hook, {})

