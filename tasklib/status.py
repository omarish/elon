import enum

class TaskStatus(enum.Enum):
    INIT = 1
    RUNNING = 2
    SUCCESS = 3
    FAILED = 4
    ERROR = 5
