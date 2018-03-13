import pickle


class BaseSerializer(object):
    pass

class PickleSerializer(BaseSerializer):
    @staticmethod
    def serialize(task_id, func, args, kwargs):
        obj = (task_id, func, args, kwargs)
        return pickle.dumps(obj)

    @staticmethod
    def deserialize(obj):
        return pickle.loads(obj)

default_serializer = PickleSerializer
serialize = default_serializer.serialize
deserialize = default_serializer.deserialize
