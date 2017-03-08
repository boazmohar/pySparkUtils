""" A set of utilities to manage pySpark SparkContext object
Assumes you have pyspark (and py4j) on the PYTHONPATH and SPARK_HOME is defined
"""
from future.utils import iteritems
import logging
import time
import sys

from multiprocessing import Process, Queue
from pyspark import SparkContext, SparkConf


def change(sc=None, app_name='customSpark', master=None, wait_for_sc=True, timeout=30, fail_on_timeout=True,
          refresh_rate=0.5, **kwargs):
    """ Returns a new Spark Context (sc) object with added properties set

    :param sc: current SparkContext
    :param app_name: name of new spark app
    :param master: url to master, if None will get from current sc
    :param kwargs:  added properties to set. In the form of key value pairs (replaces '.' with '_' in key)
                    examples: spark_task_cores='1', spark_python_worker_memory='8g'
                    see: http://spark.apache.org/docs/latest/configuration.html
    :param wait_for_sc: if to hold on returning until defaultParallelism is back to original value or timeout seconds
    :param timeout: max time in seconds to wait for new sc
    :param fail_on_timeout: whether to assert that defaultParallelism got back to a value greater then original after
                            timeout has finished
    :param refresh_rate: how long to wait in seconds between each check of the defaultParallelism
    :return: a new SparkContext
    """
    # checking input
    if master is None and sc is None:
        raise ValueError('Both master and sc are None')
    if master is None:
        master = sc.getConf().get(u'spark.master')
    if sc is not None:
        logging.getLogger('pySparkUtils').info('Original sc with %d cores' % sc.defaultParallelism)
        target_cores = sc.defaultParallelism
        sc.stop()
    else:
        target_cores = 2

    # building a new configuration with added arguments
    conf = SparkConf().setMaster(master).setAppName(app_name)
    for key in kwargs.keys():
        name = key.replace('_', '.', 100)
        value = kwargs[key]
        conf = conf.set(name, value)
        logging.getLogger('pySparkUtils').info('Setting %s to: %s' % (name, value))

    # starting the new context and waiting for defaultParallelism to get back to original value
    sc = SparkContext(conf=conf)
    if wait_for_sc:
        total_time = 0
        while sc.defaultParallelism < target_cores and total_time < timeout:
            time.sleep(refresh_rate)
            total_time += refresh_rate
        if fail_on_timeout:
            assert target_cores <= sc.defaultParallelism
    logging.getLogger('pySparkUtils').info('Returning new sc with %d cores' % sc.defaultParallelism)
    return sc


def fallback(func):
    """ Decorator function for functions that handle spark context.
        If a function changes sc we might lose it if an error occurs in the function.
        In the event of an error this decorator will log the error but return sc.
    :param func: function to decorate
    :return: decorated function
    """
    def dec(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            logging.getLogger('pySparkUtils').error('Decorator handled exception %s' % e)
            _, _, tb = sys.exc_info()
            while tb.tb_next:
                tb = tb.tb_next
            frame = tb.tb_frame
            for key, value in iteritems(frame.f_locals):
                if isinstance(value, SparkContext) and value._jsc is not None:
                    return frame.f_locals[key]
            logging.getLogger('pySparkUtils').error('Could not find SparkContext')
            return None
    return dec


def watch(func):
    """ Decorator that will abort all running spark jobs if there are failed tasks.
        It will lunch the decorated function in a different process as a daemon.
        It assumes a input variable in the decorated function of type SparkContext.
        If failed tasks are found, the process is terminated and all current scheduled jobs are aborted the function
        will return None
    :param func: function to decorate
    :return: decorated function
    """
    def dec(*args, **kwargs):
        # lunch decorated function in a separate thread
        result = Queue(1)
        p = Process(target=lambda: result.put(func(*args, **kwargs)))
        p.daemon = True
        p.start()

        # find sc variable from input params
        sc=None
        for item in tuple(args) + tuple(kwargs.values()):
            if isinstance(item, SparkContext):
                sc = item
        if sc is None:
            p.terminate()
            raise ValueError('Could not find sc in the input params')

        # get the status of all current stages
        status = sc.statusTracker()
        while result.empty():
            ids = status.getJobIdsForGroup()
            for current in ids:
                job = status.getJobInfo(current)
                for sid in job.stageIds:
                    info = status.getStageInfo(sid)
                    if info:
                        if info.numFailedTasks > 0:
                            logging.getLogger('pySparkUtils').error('Found failed tasks in %s' % func)
                            sc.cancelAllJobs()
                            p.terminate()
                            return None
            time.sleep(1)
        return result.get()
    return dec
