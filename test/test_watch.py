from pySparkUtils.utils import watch
from numpy.random import randint
import pytest
import time
import multiprocessing
pytestmark = pytest.mark.usefixtures("eng")


def test_no_fail_long(eng):
    @watch
    def test_func(sc):
        def test_inner(i):
            time.sleep(1)
            return i
        result = sc.parallelize(range(10), 10).map(test_inner)
        return result.reduce(lambda x, y: x+y)
    answer = test_func(eng)
    assert answer == sum(range(10))


def test_no_fail_short(eng):
    @watch
    def test_func(sc):
        result = sc.parallelize(range(10)).reduce(lambda x, y: x+y)
        return result
    answer = test_func(eng)
    assert answer == sum(range(10))


def test_fail(eng):
    @watch
    def test_func(sc):
        def failing(x):
            time.sleep(randint(2, 20, 1))
            return x/0
        return sc.parallelize(range(10), 10).map(failing).collect()

    result = test_func(eng)
    assert result < 0


def test_no_sc(eng):
    @watch
    def test_func(sleep_time):
        time.sleep(sleep_time)
        return
    with pytest.raises(ValueError) as excinfo:
        test_func(10)
    assert 'Could not find sc in the input params' in str(excinfo.value)
