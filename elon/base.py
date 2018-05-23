import logging
import uuid
import asyncio
from urllib.parse import urlparse

import redis
import aioredis

from .serialization import serialize_task, serialize, deserialize
from .util import singleton, now
from .status import TaskStatus
from .errors import InvalidTaskStatus


default_logger = logging.getLogger('elon')


@singleton
class ConfigMap(object):
    def __init__(self, redis_url=None, status_prefix=None, queue_name=None, worker_count=None):
        self.redis_url = redis_url
        self.status_prefix = status_prefix
        self.queue_name = queue_name
        self.worker_count = worker_count

    def load(self, **config):
        for opt in ['redis_url', 'status_prefix', 'queue_name', 'worker_count']:
            setattr(self, opt, config.get(opt))

    @property
    def redis_opts(self):
        opts = {}
        redis_info = urlparse(self.redis_url)
        opts.update({
            'host': redis_info.hostname,
            'port': redis_info.port,
            'password': redis_info.password,
        })
        if len(redis_info.path) > 0:
            opts['db'] = redis_info.path[1:]
        return opts

config = ConfigMap.instance()



class ElonBase(object):
    def __init__(self, is_async=False):
        self.config = config
        if is_async:
            loop = asyncio.get_event_loop()
            coros = [self.install_redis_async()]
            results = loop.run_until_complete(asyncio.gather(*coros))
            self.redis = results[0]
        else:
            self.redis = self.install_redis_inline()

    def install_redis_inline(self):
        return redis.StrictRedis(**self.config.redis_opts)

    async def install_redis_async(self):
        return await aioredis.create_connection(self.config.redis_url)


@singleton
class Registry(object):
    tasks = {}

    def task(self, func, **opts):
        """Decorator to turn a function into a task.

        Also registers the function in the task registry if it
        has not already.

        To use it, wrap your function with this decorator.

        @task
        def function_name_here():
            pass
        """
        if func.__name__ not in self.tasks:
            self.tasks[func.__name__] = dict(func=func, opts=opts, name=func.__name__)
        task_obj = Task(func=func)
        return task_obj

    def async_task(self, func, **opts):
        # TODO: preserve __name__ and others.
        opts.update({'is_async': True})
        if func.__name__ not in self.tasks:
            self.tasks[func.__name__] = dict(func=func, opts=opts)
        task_obj = Task(func=func)
        return task_obj


registry = Registry.instance()
task = registry.task
async_task = registry.async_task



class TaskTracker(ElonBase):
    DEFAULT_RESULT_EXPIRY = 3600

    # def __new__(cls, classname=None, mcs=[], attrdict={}):
    #     for status in TaskStatus:
    #         func_name = f'mark_{status.name.lower()}'
    #         print(f'create func {func_name}')
    #     return super().__new__(cls, classname, mcs, attrdict)

    def __init__(self, *args, logger=None, result_expiry_time=None, **kwargs):
        self.logger = logger or default_logger
        self.result_expiry_time = result_expiry_time or self.DEFAULT_RESULT_EXPIRY
        super().__init__(*args, **kwargs)

    def key_name(self, task_id):
        return ":".join([self.config.status_prefix, str(task_id)])

    def mark_status(self, task_id, new_status):
        if new_status not in TaskStatus:
            raise InvalidTaskStatus("new_status must be type of TaskStatus")
        redis_key = self.key_name(task_id)
        self.redis.expire(redis_key, self.result_expiry_time)
        self.redis.hset(redis_key, 'status', new_status)
        self.logger.info(f"{task_id} change status to {new_status}")

    def complete(self, task_id, result=None, status=TaskStatus.SUCCESS, excinfo=None):
        key_name = self.key_name(task_id)
        body = {'status': serialize(status)}
        if result:
            body['result'] = serialize(result)
        if excinfo:
            body['excinfo'] = serialize(excinfo)
        self.redis.hmset(key_name, body)

    def schedule(self, task_id, func, args, kwargs):
        serialized = serialize_task(task_id, func, args, kwargs)
        key_name = self.key_name(task_id)
        with self.redis.pipeline() as pipe:
            pipe.lpush(self.config.queue_name, serialized)
            pipe.hmset(key_name, {
                'status': serialize(TaskStatus.INIT),
                'submitted': serialize(now()),
                'result': serialize(None),
                'body': serialized
            })
            pipe.expire(key_name, self.result_expiry_time)
            pipe.execute()

    def get_task_by_uuid(self, task_id):
        return self.redis.hgetall(self.key_name(task_id))


class Task(object):
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.func_name = self.func.__name__
        self.task_id = None
        self.status = TaskStatus.INIT
        self.tracker = TaskTracker()
        self.result = None

    def __call__(self, *args, **kwargs):
        if asyncio.iscoroutinefunction(self.func):
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(asyncio.gather(self.func(*args, **kwargs)))
            return result[0]
        else:
            return self.func(*args, **kwargs)

    @classmethod
    def get(cls, task_id):
        attrs = TaskTracker().get_task_by_uuid(task_id)
        uuid, func_name, args, kwargs = deserialize(attrs[b'body'])
        registry_item = registry.tasks.get(func_name)

        task = cls(func=registry_item['func'])
        task.funcname = registry_item['name']
        task.status = deserialize(attrs[b'status'])
        task.result = deserialize(attrs[b'result'])
        task.submitted = deserialize(attrs[b'submitted'])

        body = deserialize(attrs[b'body'])
        task.args = body[2]
        task.kwargs = body[3]
        if b'excinfo' in attrs:
            task.excinfo = deserialize(attrs[b'excinfo'])

        task.task_id = task_id

        return task

    def refresh(self):
        task_id = self.task_id
        self = Task.get(task_id)
        return self

    async def call_async(self, *args, **kwargs):
        result = await self.func(*args, **kwargs)
        return result

    def __repr__(self):
        return r'<Task name=%s status=%s task_id=%s>' % (self.func_name, self.status, self.task_id)

    def enqueue_inline(self, *args, **kwargs):
        self.task_id = uuid.uuid4()
        self.tracker.schedule(self.task_id, self.func_name, args, kwargs)
        return self

    def enqueue_async(self, *args, **kwargs):
        pass

    enqueue = enqueue_inline
