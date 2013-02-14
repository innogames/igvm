_signal_handlers = {}

def send_signal(signal_name, *args, **kwargs):
    handlers = _signal_handlers.get(signal_name, [])
    for handler in handlers:
        handler(*args, **kwargs)

def register_signal(signal_name, handler):
    handlers = _signal_handlers.setdefault(signal_name, [])
    handlers.append(handler)
