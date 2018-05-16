import uuid
import asyncio
from urllib.parse import urlparse

import redis

from .serialization import serialize, deserialize
from .util import singleton
from .status import TaskStatus


@singleton
class ConfigMap(object):
    def __init__(self, redis_url=None, status_prefix=None, queue_name=None, worker_count=None):
        self.redis_url = redis_url
        self.status_prefix = status_prefix
        self.queue_name = queue_name
        self.worker_count = worker_count

    def load(self, **config):
        self.redis_url = config.get('redis_url')
        self.status_prefix = config.get('status_prefix')
        self.queue_name = config.get('queue_name')
        self.worker_count = config.get('worker_count')

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


class TaskScheduler(object):
    def __init__(self):
        self.queue_name = config.queue_name
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
        self.queue_name = config.queue_name
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
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.func_name = self.func.__name__
        self.task_id = None
        self.status = TaskStatus.INIT
        self.result = None
        self.scheduler = InlineTaskScheduler()

    def __call__(self, *args, **kwargs):
        if asyncio.iscoroutinefunction(self.func):
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(asyncio.gather(self.func(*args, **kwargs)))
            return result[0]
        else:
            return self.func(*args, **kwargs)

    async def call_async(self, *args, **kwargs):
        result = await self.func(*args, **kwargs)
        return result

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
