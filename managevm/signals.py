_signal_handlers = {}

def send_signal(signal_name, *args, **kwargs):
    handlers = _signal_handlers.get(signal_name, [])
    results = []
    for handler in handlers:
        results.append(handler(*args, **kwargs))
    return results

def register_signal(signal_name, handler):
    handlers = _signal_handlers.setdefault(signal_name, [])
    handlers.append(handler)

def on_signal(signal_name):
    """
    Decorator to register signals.
    """
    def decorator(fn):
        register_signal(signal_name, fn)
        return fn
    return decorator

