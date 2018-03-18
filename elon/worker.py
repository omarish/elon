"""Basic worker example.

You'll probably want to tune this to fit your specific needs. """

import asyncio
import aioredis

from base import TaskStatus, TaskTracker, AsyncTaskScheduler, InlineTaskScheduler, registry, config


class ExecutionError:
    pass


class Worker(object):
    def __init__(self, worker_count=32):
        self._redis = None
        self.worker_count = worker_count
        self.should_accept_work = True
        # TODO: make this an async tracker because it blocks right now.
        self.tracker = TaskTracker(logger=logger)
        self.loop = asyncio.get_event_loop()
        self.scheduler = None

    async def get_connection(self, **opts):
        redis_opts = config.redis_opts
        return await aioredis.create_connection(
            (redis_opts['host'], redis_opts['port']),
            password=redis_opts['password'],
            loop=self.loop
        )

    async def worker(self):
        conn = await self.get_connection(timeout=1)
        client = aioredis.Redis(conn)
        self.scheduler = AsyncTaskScheduler(redis_client=client)

        while 1:
            if not self.should_accept_work:
                break
            item = await self.scheduler.pop_blocking()
            if item:
                await self.process_item(*item)

    async def process_item(self, task_id, func_name, args, kwargs):
        self.tracker.mark(task_id, TaskStatus.RUNNING)
        if func_name not in registry.tasks:
            raise ExecutionError(f"Could not find task {func_name}")
        func = registry.tasks[func_name]['func']
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                await asyncio.sleep(0)
                result = func(*args, **kwargs)
        except Exception as exc:
            self.tracker.mark(task_id, TaskStatus.ERROR)
        else:
            self.tracker.mark(task_id, TaskStatus.SUCCESS)
            return result

    def spawn_workers(self, worker_count):
        return [self.loop.create_task(self.worker())
                for i in range(worker_count)]

    def run_forever(self):
        self.loop.run_until_complete(asyncio.gather(
            *self.spawn_workers(self.worker_count)))


if __name__ == '__main__':
    worker = Worker()
    worker.run_forever()
