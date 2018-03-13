from .base import task, async_task, config

"""
Expose the @task decorator so that users can import the decorator with:

`from tasklib import task`
"""

__all__ = ['task', 'async_task', 'config']
