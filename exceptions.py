class TaskError(Exception):
    pass


class RunningCompletedTaskError(TaskError):
    pass


class RunningAlreadyRunningTaskError(TaskError):
    pass


class TaskCompleted(TaskError):
    pass
