from multiprocessing import Pool, cpu_count
from enums import TaskState
from exceptions import *
from input import *
import utils


class Task(object):
    def __init__(self, func, *args, **kwargs):
        self.state = TaskState.YET_TO_START
        self.name = func.__name__
        self.task = func
        self.args = args
        self.kwargs = kwargs
        self.next_run_required = True

    def execute(self, data=None):
        return self.task(*self.args, **self.kwargs)

    def run(self):
        if not self.next_run_required:
            self.state = TaskState.COMPLETED
        if self.state == TaskState.YET_TO_START or self.state == TaskState.IDLE:
            self.state = TaskState.RUNNING
            self.next_run_required = False
            self.execute()
        elif self.state == TaskState.COMPLETED:
            raise TaskCompleted()
        elif self.state == TaskState.RUNNING:
            raise RunningAlreadyRunningTaskError()

    def add_input(self, data):
        self.input.add(data)

    def __getstate__(self):
        result = self.__dict__.copy()
        return result

    def snapshot(self):
        return self.__getstate__()

    def print_snapshot(self):
        as_string = '++++++++++++++++++++++++++++++++++++++++\n'
        as_string += '             Task Snapshot              \n'
        as_string += str(self.snapshot())
        as_string += '\n++++++++++++++++++++++++++++++++++++++++\n'
        return as_string


class BatchTask(Task):
    def __init__(self, func, batch_size, inputs, *args, **kwargs):
        self.input = Input(inputs)
        self.batch_size = batch_size
        super().__init__(func, *args, **kwargs)

    def execute(self, data):
        return self.task(data, *self.args, **self.kwargs)

    def run(self):
        if not self.next_run_required:
            self.state = TaskState.COMPLETED
        if self.state == TaskState.YET_TO_START or self.state == TaskState.IDLE:
            self.state = TaskState.RUNNING
            # here run func with actual input for current batch
            batch = utils.get_batch(self.input.data, self.batch_size)
            if batch is None or len(batch) == 0:
                self.next_run_required = False
            return self.execute(batch)
        elif self.state == TaskState.COMPLETED:
            raise TaskCompleted()
        elif self.state == TaskState.RUNNING:
            raise RunningAlreadyRunningTaskError()


class AsyncTask(BatchTask):
    def __init__(self, func, batch_size, inputs, *args, **kwargs):
        self.pool_size = cpu_count()
        self.pool = Pool(self.pool_size)
        super().__init__(func, batch_size, inputs, *args, **kwargs)

    def __getstate__(self):
        result = self.__dict__.copy()
        del result['pool']
        return result

    def worker(self, item):
        out = self.task([item], *self.args, **self.kwargs)
        return out[0] if len(out) != 0 else None

    def execute(self, data):
        return self.pool.map(self.worker, data)

    def run(self):
        if not self.next_run_required:
            self.state = TaskState.COMPLETED
        if self.state == TaskState.YET_TO_START or self.state == TaskState.IDLE:
            self.state = TaskState.RUNNING
            # here run func with actual input for current batch
            batch = utils.get_batch(self.input.data, self.batch_size)
            if batch is None or len(batch) == 0:
                self.next_run_required = False
            return self.execute(batch)
        elif self.state == TaskState.COMPLETED:
            raise TaskCompleted()
        elif self.state == TaskState.RUNNING:
            raise RunningAlreadyRunningTaskError()

