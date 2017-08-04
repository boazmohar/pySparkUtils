from pySparkUtils.utils import save_rdd_as_pickle, load_rdd_from_pickle
import os
import pytest
import thunder as td
import numpy as np
from pyspark import RDD
pytestmark = pytest.mark.usefixtures("eng")


def test_type(eng, tmpdir):
    data = td.series.fromrandom(engine=eng)
    path = os.path.join(tmpdir.dirname, 'test0')
    save_rdd_as_pickle(data, path)
    with pytest.raises(ValueError) as excinfo:
        _ = load_rdd_from_pickle(eng, path, return_type='error')
    assert 'return_type not' in str(excinfo.value)


def test_series(eng, tmpdir):
    data = td.series.fromrandom(engine=eng)
    path = os.path.join(tmpdir.dirname, 'test1')
    save_rdd_as_pickle(data, path)
    reloaded = load_rdd_from_pickle(eng, path, return_type='series')
    data_local = data.toarray()
    reloaded_local = reloaded.toarray()
    assert isinstance(reloaded, td.series.Series)
    assert np.allclose(data_local, reloaded_local)
    assert data_local.dtype == reloaded_local.dtype
    assert reloaded.npartitions() == eng.defaultParallelism


def test_images(eng, tmpdir):
    data = td.images.fromrandom(engine=eng)
    path = os.path.join(tmpdir.dirname, 'test2')
    save_rdd_as_pickle(data, path)
    reloaded = load_rdd_from_pickle(eng, path)
    data_local = data.toarray()
    reloaded_local = reloaded.toarray()
    assert isinstance(reloaded, td.images.Images)
    assert np.allclose(data_local, reloaded_local)
    assert data_local.dtype == reloaded_local.dtype
    assert reloaded.npartitions() == eng.defaultParallelism


def test_rdd(eng, tmpdir):
    data = eng.range(100)
    path = os.path.join(tmpdir.dirname, 'test3')
    save_rdd_as_pickle(data, path)
    reloaded = load_rdd_from_pickle(eng, path, return_type='rdd')
    data_local = np.array(sorted(data.collect()))
    reloaded_local = np.array(sorted(reloaded.collect()))
    assert isinstance(reloaded, RDD)
    assert np.allclose(data_local, reloaded_local)
    assert data_local.dtype == reloaded_local.dtype
    assert reloaded.getNumPartitions() == eng.defaultParallelism


def test_overwrite_true(eng, tmpdir):
    data = td.images.fromrandom(engine=eng)
    path = os.path.join(tmpdir.dirname, 'test4')
    save_rdd_as_pickle(data, path)
    save_rdd_as_pickle(data, path, overwrite=True)
    reloaded = load_rdd_from_pickle(eng, path)
    data_local = data.toarray()
    reloaded_local = reloaded.toarray()
    assert np.allclose(data_local, reloaded_local)
    assert data_local.dtype == reloaded_local.dtype
    assert reloaded.npartitions() == eng.defaultParallelism


def test_overwrite_false(eng, tmpdir):
    data = td.images.fromrandom(engine=eng)
    path = os.path.join(tmpdir.dirname, 'test5')
    save_rdd_as_pickle(data, path)
    with pytest.raises(IOError) as excinfo:
        save_rdd_as_pickle(data, path)
    assert 'already exists and overwrite is false' in str(excinfo.value)


def test_partitions(eng, tmpdir):
    data = td.images.fromrandom(engine=eng)
    path = os.path.join(tmpdir.dirname, 'test6')
    save_rdd_as_pickle(data, path)
    reloaded = load_rdd_from_pickle(eng, path,
                                    min_partitions=eng.defaultParallelism*2)
    data_local = data.toarray()
    reloaded_local = reloaded.toarray()
    assert np.allclose(data_local, reloaded_local)
    assert data_local.dtype == reloaded_local.dtype
    assert reloaded.npartitions() == eng.defaultParallelism*2
