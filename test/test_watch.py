from pySparkUtils.utils import watch
from pyspark import SparkContext
import pytest
import time
pytestmark = pytest.mark.usefixtures("eng")

def test_watch_no_sc():
    @watch
    def test_func(sleep_time):
        time.sleep(sleep_time)
        return
    with pytest.raises(ValueError) as excinfo:
        test_func(10)
    assert 'Could not find sc in the input params' in str(excinfo.value)


def test_watch_no_fail(eng):
    @watch
    def test_func(sc):
        result = sc.parallelize(range(10)).reduce(lambda x, y: x+y)
        return result
    answer = test_func(eng)
    assert answer == sum(range(10))


def test_watch_fail(eng):
    @watch
    def test_func(sc):
        def failing(x):
            time.sleep(x)
            return x/0
        return sc.parallelize(range(10)).map(failing).collect()

    result = test_func(eng)
    assert result is None