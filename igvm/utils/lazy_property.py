class lazy_property(object):
    """Decorator to lazily evaluate a property.
    The first time it is accessed, it will replace the property field
    with the calculated value."""
    def __init__(self, fn):
        self.fn = fn
        self.prop_name = fn.__name__
        self.__name__ = 'lazy__' + self.prop_name

    def __get__(self, obj, cls):
        if obj is None:
            return None
        value = self.fn(obj)
        setattr(obj, self.prop_name, value)
        return value
