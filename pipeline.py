from collections import deque
from exceptions import *
from task import *
from enums import TaskType, TaskState

def turn_into_task(batch_size):
    def decorate(func):
        def wrapper(*args, **kwargs):
            return Task(func, batch_size, *args, **kwargs)
        return wrapper
    return decorate


class Pipeline(object):
    def __init__(self):
        self.task_queue = deque()
        self.current_task = 0

    def chain_it(self, func, task_type=TaskType.BATCH, batch_size=1, inputs=None, *args, **kwargs):
        if task_type == TaskType.BATCH_ASYNC:
            task = AsyncTask(func, batch_size, inputs, *args, **kwargs)
        elif task_type == TaskType.BATCH:
            task = BatchTask(func, batch_size, inputs, *args, **kwargs)
        else:
            task = Task(func, *args, **kwargs)
        self.add_task(task)

    def add_task(self, task):
        self.task_queue.append(task)

    def get_current_task(self):
        return self.task_queue[self.current_task]

    def remove_current_task(self):
        del self.task_queue[self.current_task]

    def iterate_tasks(self):
        i = self.current_task
        yield i + 1 if i < len(self.task_queue) -1 else 0
        i += 1

    def get_next_task(self):
        return self.current_task + 1 if self.current_task < len(self.task_queue) -1 else 0

    def get_next_batch_task(self):
        for i in range(self.current_task, len(self.task_queue)):
            index = i + 1 if i < len(self.task_queue) - 1 else 0
            if index != 0 and isinstance(self.task_queue[index], BatchTask):
                return self.task_queue[index]

    def change_current_task(self):
        self.current_task = self.get_next_task()

    def reassign_input_output(self, output):
        next_task = self.get_next_batch_task()
        if next_task is not None:
            next_task.add_input(output)

    def run(self):
        output = None
        while len(self.task_queue) != 0:
            try:
                task = self.get_current_task()
                # print(task.print_snapshot())
                output = task.run()
                task.state = TaskState.IDLE
                print(output)
                # output of current task becomes input of next task
                self.reassign_input_output(output)
                self.change_current_task()
            except TaskCompleted:
                self.remove_current_task()
                # if this last task, return output of this task
                if len(self.task_queue) == 0:
                    return output
            except RunningCompletedTaskError:
                pass


def get_double(input):
    return [i * 2 for i in input]


def foo():
    print('Hello')


def plus_one(input):
    return [i + 1 for i in input]


def extend_input(input):
    return input + input


def to_string(input):
    l = [str(i) for i in input]
    return l


if __name__ == '__main__':
    pipeline = Pipeline()
    # pipeline.add_task(get_double([1,2,3,4,5]))
    BATCH_SIZE = 10
    # pipeline.chain_it(get_double, True, BATCH_SIZE, range(10))
    # pipeline.chain_it(plus_one, True, 3)
    # pipeline.chain_it(foo, False, 2)
    # pipeline.chain_it(extend_input, True, 5)
    # pipeline.chain_it(to_string, True, 5)

    pipeline.chain_it(get_double, TaskType.BATCH_ASYNC, BATCH_SIZE, range(10))
    pipeline.chain_it(plus_one, TaskType.BATCH_ASYNC, 3)
    pipeline.chain_it(foo, TaskType.SINGLE_EXECUTION, 2)
    pipeline.chain_it(extend_input, TaskType.BATCH, 5)
    pipeline.chain_it(to_string, TaskType.BATCH_ASYNC, 5)
    # pipeline.add_task(get_double(range(100)))
    # # pipeline.add_task(foo())
    # pipeline.add_task(plus_one(None))
    # pipeline.add_task(to_string(None))
    print(pipeline.run())