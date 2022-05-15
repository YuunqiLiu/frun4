import time
#import multiprocessing
import subprocess

from queue      import Queue
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from threading import Event


class Task(object):


    def __init__(self, command=""):
        super().__init__()
        self.__sub_process      = None
        self.__dependency_list  = []
        self.__finish_event     = Event()
        self.__command          = command

    def execute(self):
        print('start_to wait %s' % self.__dependency_list)
        for d in self.__dependency_list:
            d.finish_event.wait()
        print('execute')


        self.__sub_process = subprocess.Popen(self.__command, shell=True)
        #self.__sub_process = subprocess.Popen(self.__command, shell=True, executable='/bin/tcsh')
        self.__sub_process.wait()
        self.__finish_event.set()

    def add_dependency(self, task):
        self.__dependency_list.append(task)

    @property
    def finish_event(self):
        return self.__finish_event


def _TaskPool_finish_callback(queue, future):
    res = future.result()
    queue.put(res)
    print("\n%s done callback." % res)

def _TaskPool_task_wrapper(task):
    task.execute()
    return task

class TaskPool(object):

    def __init__(self, task_list=[], name='TaskPool', max_workers=20) -> None:
        super().__init__()
        self.__name                 = name
        self.__finished_task_queue  = Queue()
        self.__task_list            = task_list
        self.__thread_pool          = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="%s_" % self.__name)


    @property
    def task_num(self):
        return len(self.__task_list)

    
    def execute(self):
        for task in self.__task_list:
            future = self.__thread_pool.submit(partial(_TaskPool_task_wrapper, task))
            future.add_done_callback(partial(_TaskPool_finish_callback, self.__finished_task_queue))


    def get_res(self):
        res = self.__finished_task_queue.get()
        return res

    def shutdown(self):
        self.__thread_pool.shutdown(wait=True)




if __name__ == "__main__":


    class TestTask(Task):

        def __init__(self,delay):
            super().__init__(command="TIMEOUT /T %s /NOBREAK" % delay)

    task1 = TestTask(1)
    task2 = TestTask(2)
    task3 = TestTask(3)
    task4 = TestTask(4)
    task5 = TestTask(5)

    #task1.add_dependency(task2)
    #task2.add_dependency(task3)
    #task3.add_dependency(task4)
    #task4.add_dependency(task5)

    task_list = []
    task_list.append(task1)
    task_list.append(task2)
    task_list.append(task3)
    task_list.append(task4)
    task_list.append(task5)
    #for i in range(10,0,-1):
    #    task_list.append(TestTask(i))
    pool = TaskPool(task_list)

    print("Start to send task.")
    pool.execute()
    print("All task started.")
    for _ in range(pool.task_num):
        print('wait')
        print('get %s result.' % pool.get_res())

    pool.shutdown()
    print('Test done.')


