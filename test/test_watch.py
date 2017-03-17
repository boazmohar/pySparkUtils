from pySparkUtils.utils import watch
import pytest
import time
pytestmark = pytest.mark.usefixtures("eng")


def test_no_sc():
    @watch
    def test_func(sleep_time):
        time.sleep(sleep_time)
        return
    with pytest.raises(ValueError) as excinfo:
        test_func(10)
    assert 'Could not find sc in the input params' in str(excinfo.value)


@pytest.mark.skipif(multiprocessing.cpu_count() < 2,
                    reason="requires more then 2 cores")
def test_no_fail_long(eng):
    @watch
    def test_func(sc):
        def test_inner(i):
            time.sleep(0.1)
            return i
        result = sc.parallelize(range(100)).map(test_inner).reduce(lambda x, y: x+y)
        return result
    answer = test_func(eng)
    assert answer == sum(range(100))


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
            time.sleep(x)
            return x/0
        return sc.parallelize(range(10)).map(failing).collect()

    result = test_func(eng)
    assert result is None
