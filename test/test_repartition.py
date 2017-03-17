from pySparkUtils.utils import balanced_repartition
import pytest
import thunder as td
import numpy as np
from pyspark import RDD

pytestmark = pytest.mark.usefixtures("eng")


def test_wrong_type():
    with pytest.raises(ValueError) as excinfo:
        balanced_repartition(range(10), 10)
    assert 'Wrong data type, expected [RDD, Images, Series] got' in str(excinfo.value)


def test_thunder_images(eng):
    array = np.random.rand(10, 20, 30)
    data = td.images.fromarray(array, engine=eng, npartitions=2)
    assert data.tordd().glom().map(len).collect() == [5, 5]
    data2 = balanced_repartition(data, 5)
    assert isinstance(data2, td.images.Images)
    assert data2.tordd().glom().map(len).collect() == [2, 2, 2, 2, 2]


def test_thunder_series(eng):
    array = np.random.rand(10, 20, 30)
    data = td.series.fromarray(array, engine=eng, npartitions=2)
    assert data.tordd().glom().map(len).collect() == [100, 100]
    data2 = balanced_repartition(data, 5)
    assert isinstance(data2, td.series.Series)
    assert data2.tordd().glom().map(len).collect() == [40, 40, 40, 40, 40]


def test_rdd(eng):
    data = eng.parallelize(range(100), 2)
    assert data.glom().map(len).collect() == [50, 50]
    data2 = balanced_repartition(data, 5)
    assert isinstance(data2, RDD)
    assert data2.glom().map(len).collect() == [20, 20, 20, 20, 20]
