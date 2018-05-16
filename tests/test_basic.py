import elon
from elon import task
from elon import config as elon_config

elon_config.redis_url = "redis://localhost:6379/0"
elon_config.work_queue = "work_queue"
elon_config.status_prefix = "task_result"


@task
def wrapped_function(a, b, c):
    return a + b + c

def test_basic():
    assert hasattr(wrapped_function, 'enqueue')
    assert wrapped_function(1, 2, 3) == 6
    result = wrapped_function.enqueue(2, 3, 4)
    assert isinstance(result, elon.base.Task)

# @task
# def wrapped_function2(a, b, c):
#     return a + b + c

# def test_named_task():
#     assert wrapped_function2.__name__ == 'darth_vader'
