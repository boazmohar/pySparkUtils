import pytest
from pyspark import SparkContext
import os

@pytest.fixture()
def eng():
    if 'SPARK_HOME' not in os.environ:
        import pyspark
        path1 = pyspark.__file__
        dir1 = os.path.dirname(path1)
        parent = os.path.split(dir1)[0]
        spark_home = os.path.split(parent)[0]
        os.environ['SPARK_HOME'] = spark_home
    sc = SparkContext()
    yield sc
    sc.stop()
    del sc
