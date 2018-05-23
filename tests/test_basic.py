import elon
from elon import task
from elon import config as elon_config
from elon.base import TaskTracker, Task
from elon.status import TaskStatus

elon_config.redis_url = "redis://localhost:6379/0"
elon_config.queue_name = "work_queue"
elon_config.status_prefix = "task_result"


def complete_task(t):
    tracker = TaskTracker()
    result = t(*t.args, **t.kwargs)
    tracker.complete(t.task_id, result=result)


def fail_task(t):
    tracker = TaskTracker()
    result = "epic fail"
    tracker.complete(t.task_id, result=result, status=TaskStatus.ERROR, excinfo=Exception("there was an error"))

@task
def func_to_test(a, b, c):
    return a + b + c


def test_basic():
    assert hasattr(func_to_test, 'enqueue')
    assert func_to_test(1, 2, 3) == 6
    result = func_to_test.enqueue(2, 3, 4)
    assert isinstance(result, elon.base.Task)
    assert result.task_id is not None

    task_ = Task.get(result.task_id)
    assert task_.status == TaskStatus.INIT

    complete_task(task_)

    task_ = Task.get(result.task_id)
    assert task_.status == TaskStatus.SUCCESS
    assert task_.result == 9  # 2+3+4 from above
    assert task_.submitted is not None


def test_failure():
    result = func_to_test.enqueue(3, 4, 5)
    task_ = Task.get(result.task_id)
    fail_task(task_)

    task_ = Task.get(result.task_id)
    assert task_.status == TaskStatus.ERROR
    assert isinstance(task_.excinfo, Exception)
