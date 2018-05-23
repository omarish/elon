import enum

class TaskStatus(enum.Enum):
    INIT = 10
    ENQUEUED = 20
    RUNNING = 30
    SUCCESS = 40
    FAILED = 50
    ERROR = 60
