
class MetricRegistry:

    _metrics = {}

    @classmethod
    def register(cls, name):
        # TODO: error if name exists
        def decorator(metric_cls):
            cls._metrics[name] = metric_cls
            return metric_cls
        return decorator

    @classmethod
    def get(cls, item):
        return cls._metrics[item]
