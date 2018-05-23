import pickle


class BaseSerializer(object):
    pass

class PickleSerializer(BaseSerializer):
    @staticmethod
    def serialize_task(task_id, func, args, kwargs):
        obj = (task_id, func, args, kwargs)
        return pickle.dumps(obj)

    @staticmethod
    def serialize(*args, **kwargs):
        return pickle.dumps(*args, **kwargs)

    @staticmethod
    def deserialize(obj):
        return pickle.loads(obj)

default_serializer = PickleSerializer
serialize = default_serializer.serialize
serialize_task = default_serializer.serialize_task
deserialize = default_serializer.deserialize
