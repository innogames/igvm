from igvm.signals import register_signal

def print_config(config):
    if 'print' in config:
        print config

register_signal('config_finished', print_config)
