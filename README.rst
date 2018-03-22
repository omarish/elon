Elon
====

Elon is a lightweight async job queue backed by redis.

Why the name?
-------------

Because Elon Musk gets things done.

What it does?
-------------

Say you have a Django or Flask application that is heavily network- or
IO-bound. Say in this app you have a view that makes a slow backend API
call. When a user visits this view, your web worker gets tied up until
the API responds, which severely limits your throughput. In some cases,
you must wait for the API's response to generate your response (in which
case I'd recommend Tornado), but in others you might not need the
response right away, and in this case, Elon is perfect for your
situation.

Example
-------

Here is a long-running task - pretend that instead of waiting 10
seconds, it's actually hitting a backend API.

.. code:: python

    import time
    def long_api_request():
        time.sleep(10)

We could rewrite it as an async task using elon:

.. code:: python

    import asyncio
    from tasklib import task

    @task
    async def long_api_request():
        await asyncio.sleep(10)

When it comes to calling the task, before:

.. code:: python

    @app.route('/enqueue_task')
    def enqueue_task():
        # Runs the task and returns once it is complete.
        long_running_process()
        return 'Success!'

After:

.. code:: python

    @app.route('/enqueue_task')
    def enqueue_task():
        # Enqueue the task and return instantly.
        long_running_process.enqueue()
        return 'Success!'

When you call ``enqueue()`` on a task, you'll instantly receive a UUID,
which you can use to query for the result and see job progress.

Decorating classes:

You can also decorate classes, as long as they inherit from ``Task``.
Example:

.. code:: python

    class ComplexTask(Task):
        def execute(self):
            pass

Classes that inherit from Task must define their own ``execute`` method
- this is the method run to actually call the task.
