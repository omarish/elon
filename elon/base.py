import uuid
import aioredis
from urllib.parse import urlparse

import redis

from .serialization import serialize, deserialize
from .util import singleton
from .status import TaskStatus


@singleton
class ConfigMap(object):
    WRITABLE_OPTIONS = ('redis_url', 'work_queue', 'status_prefix')
    data = {}

    def __init__(self, **opts):
        for option in self.WRITABLE_OPTIONS:
            self.data[option] = None

    def update(self, opts):
        self.data.update(opts)

    def __getattr__(self, x, default=None):
        value = self.data.get(x, default)
        if not value:
            raise AttributeError()
        return value

    @property
    def redis_opts(self):
        opts = {}
        if self.data['redis_url']:
            redis_info = urlparse(self.data['redis_url'])
            opts.update({
                'host': redis_info.hostname,
                'port': redis_info.port,
                'password': redis_info.password,
            })
            if len(redis_info.path) > 0:
                opts['db'] = redis_info.path[1:]
        return opts

config = ConfigMap.instance()


class TaskScheduler(object):
    def __init__(self):
        self.queue_name = config.work_queue
        self._redis = None

    @property
    def redis(self):
        if not self._redis:
            self._redis = redis.StrictRedis(**config.redis_opts)
        return self._redis


class InlineTaskScheduler(TaskScheduler):
    def schedule(self, task_id, func, args, kwargs):
        self.redis.lpush(self.queue_name, serialize(task_id, func, args, kwargs))

    def pop(self):
        item = self.redis.rpop(self.queue_name)
        if item:
            task_id, func_name, args, kwargs = deserialize(item)
            return (task_id, func_name, args, kwargs)
        else:
            return None


class AsyncTaskScheduler(object):
    def __init__(self, redis_client):
        self.queue_name = config.work_queue
        # TODO: raise if not async connection
        self.redis = redis_client

    async def schedule(self, task_id, func, args=(), kwargs={}):
        serialized = serialize(task_id, func, args, kwargs)
        await self.redis.lpush(self.queue_name, serialized)

    async def pop(self):
        value = await self.redis.rpop(self.queue_name)
        if value:
            # (task_id, func_name, args, kwargs)
            return deserialize(value)

    async def pop_blocking(self, timeout=10):
        value = await self.redis.brpop(self.queue_name, timeout=timeout)
        if value:
            return deserialize(value[1])


class TaskTracker(TaskScheduler):
    expiry_time = 60

    def __init__(self, *args, logger=None, **kwargs):
        self.logger = logger
        super().__init__(*args, **kwargs)

    def key_name(self, task_id):
        return ":".join([config.status_prefix, str(task_id)])

    def mark(self, task_id, new_status):
        if new_status not in TaskStatus:
            raise "new_status must be type of TaskStatus"
        rkey = self.key_name(task_id)
        self.redis.expire(rkey, self.expiry_time)
        self.redis.hset(rkey, 'status', new_status)
        self.logger.info(f"{task_id} change status to {new_status}")


class Task(object):
    def enqueue_inline(self, *args, **kwargs):
        self.task_id = uuid.uuid4()
        self.scheduler.schedule(self.task_id, self.func_name, args, kwargs)
        return self

class Task(object):
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.func_name = self.func.__name__
        self.task_id = None
        self.status = TaskStatus.INIT
        self.result = None
        self.scheduler = InlineTaskScheduler()

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self):
        return r'<Task name=%s status=%s task_id=%s>' % (self.func_name, self.status, self.task_id)

    def enqueue_inline(self, *args, **kwargs):
        self.task_id = uuid.uuid4()
        self.scheduler.schedule(self.task_id, self.func_name, args, kwargs)
        return self

    def enqueue_async(self, *args, **kwargs):
        pass

    enqueue = enqueue_inline


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
            self.tasks[func.__name__] = dict(func=func, opts=opts)
        task_obj = Task(func=func)
        return task_obj

    def async_task(self, func, **opts):
        # TODO: preserve __name__ and others.
        opts.update({ 'is_async': True })
        if func.__name__ not in self.tasks:
            self.tasks[func.__name__] = dict(func=func, opts=opts)
        task_obj = Task(func=func)
        return task_obj

registry = Registry.instance()
task = registry.task
async_task = registry.async_task