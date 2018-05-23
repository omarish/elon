"""Basic processing worker.

You may want to tune this slightly to fit your app's specific needs.
Nonetheless, it's a good starting point.
"""

import asyncio
import logging

from .base import TaskStatus, registry, config
from .tracker import TaskTracker, AsyncTaskTracker

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ExecutionError(Exception):
    pass


class Worker(object):
    def __init__(self, worker_count, is_async=False):
        self._redis = None
        self.worker_count = worker_count
        self.should_accept_work = True
        if is_async:
            self.tracker = AsyncTaskTracker()
        else:
            self.tracker = TaskTracker()
        self.loop = asyncio.get_event_loop()
        self.scheduler = None

    async def worker(self, number):
        while 1:
            if not self.should_accept_work:
                break
            item = await self.tracker.get()
            if item:
                result = await self.process_item(*item)
            else:
                print(f"worker {number} got nothing, sleeping")
                await asyncio.sleep(1)

    async def process_item(self, task_id, func_name, args, kwargs):
        await self.tracker.mark_status(task_id, TaskStatus.RUNNING)
        if func_name not in registry.tasks:
            logger.warn(f"Could not find task {func_name}")
        else:
            func = registry.tasks[func_name]['func']
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    await asyncio.sleep(0)
                    result = func(*args, **kwargs)
            except Exception as exc:
                await self.tracker.complete(task_id, result=None, status=TaskStatus.ERROR, excinfo=exc)
            else:
                await self.tracker.complete(task_id, result=result)
                return result

    def run_forever(self):
        workers = [self.loop.create_task(self.worker(i)) for i in range(self.worker_count)]
        self.loop.run_until_complete(asyncio.gather(*workers))


if __name__ == '__main__':
    config.redis_url = "redis://localhost:6379/0"
    config.queue_name = "work_queue"
    config.status_prefix = "task_result"

    worker = Worker(4, is_async=True)
    worker.run_forever()
