# Elon

Elon is a lightweight async job queue backed by redis.

## Read this first!

This is in pre-alpha. Please use at your own risk.

## Why the name?

Because Elon Musk gets things done.

## What it does?

Say you have a Django or Flask application that is very network- or IO-bound. For example, your view might query a slow API. In this case, your web worker gets tied up until the API responds, which severely limits your throughput. In some cases, you must wait for the API's response to generate your response (in which case I'd recommend Tornado), but in others you might not need the response right away.

Here is a long running task:

```python
import time
def long_api_request():
    time.sleep(10)
```

We could rewrite it as an async task:

```python
import asyncio
from tasklib import task

@task
async def long_api_request():
    await asyncio.sleep(10)
```

Before:

```python
@app.route('/enqueue_task')
def enqueue_task():
    # Runs the task and returns once it is complete.
    long_running_process()
    return 'Success!'
```

After:

```python
@app.route('/enqueue_task')
def enqueue_task():
    # Enqueue the task and return instantly.
    long_running_process.enqueue()
    return 'Success!'
```

When you call `enqueue()` on a task, you'll instantly receive a UUID, which you can use to query for the result and see job progress.

Decorating classes:

You can also decorate classes, as long as they inherit from `Task`. Example:

```python
class ComplexTask(Task):
    def execute(self):
        pass
```

Classes that inherit from Task must define their own `execute` method - this is the method run to actually call the task.
