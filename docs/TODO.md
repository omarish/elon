# TODO:

## Subclass Task to create tasks.

Motivation: there are a lot of tasks that are too complex to be expressed in a
single function, and I think the solution is to make tasks subclass-able.

For example:

```python
class VeryComplexTask(elon.Task):
    def execute(self, *args, **kwargs):
        pass

    def utility_function(self):
        # Accessible from `execute()`
        pass
```

## Update `Task.status` as the task progresses


We have a placeholder for this, but the task status doesn't actually update yet.

## Support `name` and other arguments in `@task` decorator

```python
@task(name='something_nice')
def long_name_that_we_dont_want(self):
    pass
```
