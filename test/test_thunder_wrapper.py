from pySparkUtils.utils import thunder_decorator
import pytest
import thunder as td
from pyspark import RDD

pytestmark = pytest.mark.usefixtures("eng")


def test_no_input():
    @thunder_decorator
    def test_func(arg1, arg2, arg3, arg4):
        return 1

    with pytest.raises(ValueError) as excinfo:
        test_func(1, 2, arg3=3, arg4=4)
    assert 'Wrong data type, expected [RDD, Images, Series] got' in \
           str(excinfo.value)


def test_two_inputs(eng):
    @thunder_decorator
    def test_func(arg1, arg2, arg3, arg4):
        return 1

    images = td.images.fromrandom(engine=eng)
    series = td.series.fromrandom(engine=eng)
    with pytest.raises(ValueError) as excinfo:
        test_func(1, 2, arg3=images, arg4=images)
    assert 'Expecting on input argument of type Series / Images, got:' in\
           str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        test_func(1, images, arg3=3, arg4=series)
    assert 'Expecting on input argument of type Series / Images, got:' in\
           str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        test_func(1, series, arg3=3, arg4=images)
    assert 'Expecting on input argument of type Series / Images, got:' in\
           str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        test_func(images, images, arg3=3, arg4=4)
    assert 'Expecting on input argument of type Series / Images, got:' in\
           str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        test_func(series, series, arg3=3, arg4=4)
    assert 'Expecting on input argument of type Series / Images, got:' in\
           str(excinfo.value)


def test_no_rdd_output(eng):
    @thunder_decorator
    def test_func(arg1, arg2, arg3, arg4):
        return 4

    images = td.images.fromrandom(engine=eng)
    result = test_func(1, 2, arg3=images, arg4=4)
    assert result == 4

    @thunder_decorator
    def test_func2(arg1, arg2, arg3, arg4):
        return 4, 5

    result2 = test_func2(1, 2, arg3=images, arg4=4)
    assert result2 == (4, 5)

    @thunder_decorator
    def test_func3(arg1, arg2, arg3, arg4):
        return

    result3 = test_func3(1, 2, arg3=images, arg4=4)
    assert result3 is None


def test_two_outputs(eng):
    @thunder_decorator
    def test_func(arg1, arg2, arg3, arg4):
        return arg3, arg3

    images = td.images.fromrandom(engine=eng)
    with pytest.raises(ValueError) as excinfo:
        test_func(1, 2, arg3=images, arg4=4)
    assert 'Expecting one RDD as output got:' in str(excinfo.value)


def test_images(eng):
    @thunder_decorator
    def test_func(arg1, arg2, arg3, arg4):
        assert isinstance(arg3, RDD)
        return 1, 2, arg3, 4

    images = td.images.fromrandom(engine=eng)
    assert isinstance(images, td.images.Images)
    result = test_func(1, 2, arg3=images, arg4=4)
    assert isinstance(result[2], td.images.Images)

    @thunder_decorator
    def test_func2(arg1, arg2, arg3, arg4):
        assert isinstance(arg2, RDD)
        return 1, arg2, 3, 4

    result = test_func2(1, images, arg3=3, arg4=4)
    assert isinstance(result[1], td.images.Images)


def test_series(eng):
    @thunder_decorator
    def test_func(arg1, arg2, arg3, arg4):
        assert isinstance(arg3, RDD)
        return 1, 2, arg3, 4

    series = td.series.fromrandom(engine=eng)
    assert isinstance(series, td.series.Series)
    result = test_func(1, 2, arg3=series, arg4=4)
    assert isinstance(result[2], td.series.Series)

    @thunder_decorator
    def test_func2(arg1, arg2, arg3, arg4):
        assert isinstance(arg2, RDD)
        return 1, arg2, 3, 4

    result = test_func2(1, series, arg3=3, arg4=4)
    assert isinstance(result[1], td.series.Series)


def test_rdd(eng):
    @thunder_decorator
    def test_func(arg1, arg2, arg3, arg4):
        return 1, 2, arg3, 4

    rdd = td.series.fromrandom(engine=eng).tordd()
    assert isinstance(rdd, RDD)
    result = test_func(1, 2, arg3=rdd, arg4=4)
    assert isinstance(result[2], RDD)

    @thunder_decorator
    def test_func2(arg1, arg2, arg3, arg4):
        return 1, arg2, 3, 4

    result = test_func2(1, rdd, arg3=3, arg4=4)
    assert isinstance(result[1], RDD)
    print('1')

