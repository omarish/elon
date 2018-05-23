import asyncio
import redis
import aioredis
import logging

from .serialization import serialize_task, serialize, deserialize
from .status import TaskStatus
from .errors import InvalidTaskStatus
from .util import now


class TaskTrackerBase(object):
    def __init__(self, *args, logger=None):
        from .base import config
        self.config = config
        self.logger = logger or logging.getLogger()

    def key_name(self, task_id):
        return ":".join([self.config.status_prefix, str(task_id)])

    # def mark_status(self, task_id, new_status):
    #     if new_status not in TaskStatus:
    #         raise InvalidTaskStatus("new_status must be type of TaskStatus")

    def complete(self, task_id, result=None, status=TaskStatus.SUCCESS, excinfo=None):
        key_name = self.key_name(task_id)
        body = {'status': serialize(status)}
        if result:
            body['result'] = serialize(result)
        if excinfo:
            body['excinfo'] = serialize(excinfo)
        return (key_name, body)

    def schedule(self, task_id, func, args, kwargs):
        raise NotImplementedError("implement in subclass")

    def get_task_by_uuid(self, task_id):
        raise NotImplementedError()

    def empty(self):
        raise NotImplementedError()


class TaskTracker(TaskTrackerBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis = redis.StrictRedis(**self.config.redis_opts)

    def mark_status(self, task_id, new_status):
        redis_key = self.key_name(task_id)
        self.redis.expire(redis_key, self.config.result_expiry_time)
        self.redis.hset(redis_key, 'status', new_status)

    def complete(self, *args, **kwargs):
        key_name, body = super().complete(*args, **kwargs)
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
            pipe.expire(key_name, self.config.result_expiry_time)
            pipe.execute()

    def get_task_by_uuid(self, task_id):
        return self.redis.hgetall(self.key_name(task_id))

    def empty(self):
        return self.redis.llen(self.config.queue_name) == 0

    def get(self, block=False, timeout=None):
        if block:
            item = self.redis.brpop(self.config.queue_name, timeout=timeout or 0)
        else:
            item = self.redis.rpop(self.config.queue_name)

        if item:
            task_id, func_name, args, kwargs = deserialize(item)
            return (task_id, func_name, args, kwargs)
        return None


class AsyncTaskTracker(TaskTrackerBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        result = asyncio.get_event_loop().run_until_complete(asyncio.gather(self._install_redis()))
        self.redis = aioredis.Redis(result[0])

    async def _install_redis(self):
        return await aioredis.create_connection(self.config.redis_url)

    async def mark_status(self, task_id, new_status):
        redis_key = self.key_name(task_id)
        await self.redis.expire(redis_key, self.config.result_expiry_time)
        await self.redis.hset(redis_key, 'status', serialize(new_status))
        return 1

    async def complete(self, *args, **kwargs):
        key_name, body = super().complete(*args, **kwargs)
        # import pdb; pdb.set_trace()
        await self.redis.hmset_dict(key_name, body)

    async def schedule(self, task_id, func, args, kwargs):
        serialized = serialize_task(task_id, func, args, kwargs)
        key_name = self.key_name(task_id)

        pipe = self.redis.pipeline()
        pipe.lpush(self.config.queue_name, serialized)
        pipe.hmset_dict(key_name, {
            'status': serialize(TaskStatus.INIT),
            'submitted': serialize(now()),
            'result': serialize(None),
            'body': serialized
        })
        pipe.expire(key_name, self.config.result_expiry_time)
        await pipe.execute()

    async def get_task_by_uuid(self, task_id):
        return await self.redis.hgetall(self.key_name(task_id))

    async def empty(self):
        return (await self.redis.llen(self.config.queue_name)) == 0

    async def get(self, block=False):
        if block:
            item = await self.redis.brpop(self.config.queue_name, timeout=10)
        else:
            item = await self.redis.rpop(self.config.queue_name)
            while item is None:
                await asyncio.sleep(1)
                item = await self.redis.rpop(self.config.queue_name)

        task_id, func_name, args, kwargs = deserialize(item)
        return (task_id, func_name, args, kwargs)
