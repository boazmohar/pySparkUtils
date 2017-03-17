import pytest
from pyspark import SparkContext
import os
import multiprocessing


@pytest.fixture()
def eng():
    if 'SPARK_HOME' not in os.environ:
        import pyspark
        path1 = pyspark.__file__
        dir1 = os.path.dirname(path1)
        parent = os.path.split(dir1)[0]
        spark_home = os.path.split(parent)[0]
        os.environ['SPARK_HOME'] = spark_home
    # figure out the number of cores to use
    cores = multiprocessing.cpu_count()
    if cores <= 2:
        raise RuntimeWarning('some test would fail with only 2 cores')
    sc = SparkContext(master='local[' + str(cores-1) + ']')
    yield sc
    sc.stop()
    del sc
