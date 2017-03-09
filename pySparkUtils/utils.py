""" A set of utilities to manage pySpark SparkContext object
Assumes you have pyspark (and py4j) on the PYTHONPATH and SPARK_HOME is defined
"""
from future.utils import iteritems
import collections
import logging
import time
import shutil
import sys
import os
import thunder as td
import numpy as np
from multiprocessing import Process, Queue
from pyspark import SparkContext, SparkConf, RDD


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
            return func(*args, **kwargs)
        except Exception as e:
            logging.getLogger('pySparkUtils').error('Decorator handled exception %s' % e, exc_info=True)
            _, _, tb = sys.exc_info()
            while tb.tb_next:
                tb = tb.tb_next
            frame = tb.tb_frame
            for key, value in iteritems(frame.f_locals):
                if isinstance(value, SparkContext) and value._jsc is not None:
                    return frame.f_locals[key]
            logging.getLogger('pySparkUtils').error('Could not find SparkContext', exc_info=True)
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

        # find sc variable from input params
        sc=None
        for item in tuple(args) + tuple(kwargs.values()):
            if isinstance(item, SparkContext):
                sc = item
        if sc is None:
            raise ValueError('Could not find sc in the input params')

        # get the status of all current stages
        status = sc.statusTracker()
        p.start()
        while result.empty():
            flag = False
            ids = status.getJobIdsForGroup()
            if not result.empty():
                break
            for current in ids:
                job = status.getJobInfo(current)
                if not result.empty() or flag:
                    break
                for sid in job.stageIds:
                    if not result.empty():
                        flag = True
                        break
                    info = status.getStageInfo(sid)
                    if info:
                        if info.numFailedTasks > 0:
                            logging.getLogger('pySparkUtils').info(info)
                            logging.getLogger('pySparkUtils').error('Found failed tasks at: %s' % info.name)
                            sc.cancelAllJobs()
                            p.terminate()
                            return None
            time.sleep(1)
        return result.get()
    return dec


def thunder_decorator(func):
    """ Decorator for functions so they could get as input a thunder.Images / thunder.Series object,
    while they are expecting an rdd. Also will return the data from rdd to the appropriate type
    Assumes only one input object of type Images/Series, and up to one output object of type RDD

    :param func: function to decorate
    :return: decorated function
    """
    def dec(*args, **kwargs):
        # find Images / Series object in args
        args = list(args)
        image_args = list(map(lambda x: isinstance(x, td.images.Images), args))
        series_args = list(map(lambda x: isinstance(x, td.series.Series), args))
        rdd_args = list(map(lambda x: isinstance(x, RDD), args))

        #find Images / Series object in kwargs
        image_kwargs = []
        series_kwargs = []
        rdd_kwargs = []
        for key, value in iteritems(kwargs):
            if isinstance(value, td.images.Images):
                image_kwargs.append(key)
            if isinstance(value,  td.series.Series):
                series_kwargs.append(key)
            if isinstance(value,  RDD):
                rdd_kwargs.append(key)

        # make sure there is only one
        count = sum(image_args) + sum(series_args) + sum(rdd_args) +\
                len(image_kwargs) + len(series_kwargs) + len(rdd_kwargs)
        if count == 0:
            raise ValueError('Wrong data type, expected [RDD, Images, Series] got None')
        if count > 1:
            raise ValueError('Expecting on input argument of type Series / Images, got: %d' % count)

        # bypass for RDD
        if  sum(rdd_args) or len(rdd_kwargs):
            return func(*args, **kwargs)
        image_flag = None
        # convert to rdd and send
        if sum(image_args) > 0:
            image_flag = True
            index = np.where(image_args)[0][0]
            args[index] = args[index].tordd()
            result = func(*args, **kwargs)
        if sum(series_args) > 0:
            image_flag = False
            index = np.where(series_args)[0][0]
            args[index] = args[index].tordd()
            result = func(*args, **kwargs)

        if len(image_kwargs) > 0:
            image_flag = True
            kwargs[image_kwargs[0]] = kwargs[image_kwargs[0]].tordd()
            result = func(*args, **kwargs)

        if len(series_kwargs) > 0:
            image_flag = False
            kwargs[series_kwargs[0]] = kwargs[series_kwargs[0]].tordd()
            result = func(*args, **kwargs)

        if image_flag is None:
            raise RuntimeError('Target function did not run')

        #handle output
        if not isinstance(result, tuple):
            result = (result,)
        result_len = len(result)
        rdd_index = np.where(list(map(lambda x: isinstance(x, RDD), result)))[0]

        # no RDD as output
        if len(rdd_index) == 0:
            logging.getLogger('pySparkUtils').debug('No RDDs found in output')
            if result_len == 1:
                return result[0]
            else:
                return result

        if len(rdd_index) > 1:
            raise ValueError('Expecting one RDD as output got: %d' % len(rdd_index))
        result = list(result)
        rdd_index = rdd_index[0]

        # handle type of output
        if image_flag:
            result[rdd_index] = td.images.fromrdd(result[rdd_index])
        else:
            result[rdd_index] = td.series.fromrdd(result[rdd_index])
        if result_len == 1:
            return result[0]
        else:
            return result

    dec.__doc__ = func.__doc__
    dec.__repr__ = func.__repr__
    return dec


@thunder_decorator
def balanced_repartition(data, partitions):
    """ balanced_repartition(data, partitions)
    Reparations an RDD making sure data is evenly distributed across partitions
    for Spark version < 2.1 (see: https://issues.apache.org/jira/browse/SPARK-17817)

    :param data: RDD
    :param partitions: number of partition to use
    :return: repartitioned data
    """
    def repartition(data_inner, partitions_inner):
        # repartition by zipping an index to the data, repartition by % on it and removing it
        data_inner = data_inner.zipWithIndex().map(lambda x: (x[1], x[0]))
        data_inner = data_inner.partitionBy(partitions_inner, lambda x: x % partitions_inner)
        return data_inner.map(lambda x: x[1])

    if isinstance(data, RDD):
        return repartition(data, partitions)
    else:
        raise ValueError('Wrong data type, expected [RDD, Images, Series] got: %s' % type(data))


@thunder_decorator
def regroup(rdd, groups=10, check_first=False):
    """ Regroup an rdd using a new key added that is 0-numGtoup-1

    :param rdd: input rdd as a (k,v) pairs
    :param groups: number of groups to concatenate to
    :param check_first: check if first value is a key value pair.
    :return: a new rdd in the form of (groupNum, list of (k, v) in that group) pairs

    Example:
    >>>data = sc.parallelize(zip(range(4), range(4)))
    >>>data.collect()
    >>> [(0, 0), (1, 1), (2, 2), (3, 3)]
    >>>data2 = regroup(data, 2)
    >>>data2.collect()
    >>> [(0, [(0, 0), (2, 2)]), (1, [(1, 1), (3, 3)])]
    """
    if check_first:
        first = rdd.first()
        if isinstance(first, (list, tuple, collections.Iterable)):
            if len(first) != 2:
                raise ValueError('first item was not not length 2: %d' % len(first))
        else:
            raise ValueError('first item was wrong type: %s' % type(first))
    rdd = rdd.map(lambda kv: (kv[0] % groups, (kv[0], kv[1])), preservesPartitioning=True)
    return rdd.groupByKey().mapValues(list)


@thunder_decorator
def save_rdd_as_pickle(rdd, path, batch_size=10, overwrite=False):
    """ Saves an rdd by grouping all the records of each partition as one pickle file

    :param rdd: rdd to save
    :param path: where to save
    :param batch_size: batch size to pass to spark saveAsPickleFile
    :param overwrite: if directory exist whether to overwrite
    """
    if os.path.isdir(path):
        if overwrite:
            logging.getLogger('pySparkUtils').info('Deleting files from: %s' % path)
            shutil.rmtree(path)
            logging.getLogger('pySparkUtils').info('Done deleting files from: %s' % path)
        else:
            logging.getLogger('pySparkUtils').error('Directory %s already exists and overwrite is false' % path)
            raise IOError('Directory %s already exists and overwrite is false' % path)
    rdd.glom().saveAsPickleFile(path, batchSize=batch_size)
    logging.getLogger('pySparkUtils').info('Saved rdd as pickle to: %s' % path)


def load_rdd_from_pickle(sc, path, min_partitions=None, return_type='images'):
    """  Loads an rdd that was saved as one pickle file per partition

    :param sc: Spark Context
    :param path: directory to load from
    :param min_partitions: minimum number of partitions. if None will be sc.defaultParallelism
    :param return_type: what to return. 'rdd' - RDD, 'images' - Thunder Images object, 'series' - Thunder Series object
    :return: based on return type.
    """
    if min_partitions is None:
        min_partitions = sc.defaultParallelism
    rdd = sc.pickleFile(path, minPartitions=min_partitions)
    rdd = rdd.flatMap(lambda x: x)
    if return_type == 'images':
        result = td.images.fromrdd(rdd).repartition(min_partitions)
    elif  return_type == 'series':
        result = td.series.fromrdd(rdd).repartition(min_partitions)
    elif  return_type == 'rdd':
        result = rdd.repartition(min_partitions)
    else:
        raise ValueError('return_type not supported: %s' % return_type)
    logging.getLogger('pySparkUtils').info('Loaded rdd from: %s as type: %s' % (path, return_type))
    return result
