from enum import Enum


class TaskType(Enum):
    SINGLE_EXECUTION = 1
    BATCH = 2
    BATCH_ASYNC = 3


class TaskState(Enum):
    YET_TO_START = 1
    RUNNING = 2
    IDLE = 3
    COMPLETED = 4