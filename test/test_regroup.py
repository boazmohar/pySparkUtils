from pySparkUtils.utils import regroup
import pytest
pytestmark = pytest.mark.usefixtures("eng")


def test_regroup(eng):
    data = eng.parallelize(zip(range(4), range(4)))
    data2 = regroup(data, 2).collect()
    assert data2 == [(0, [(0, 0), (2, 2)]), (1, [(1, 1), (3, 3)])]


def test_not_key_value(eng):
    data = eng.parallelize(range(4))
    with pytest.raises(ValueError) as excinfo:
        _ = regroup(data, 2, check_first=True).collect()
    assert 'first item was wrong type' in str(excinfo.value)

    data2 = eng.parallelize(zip(range(4), range(4), range(4)))
    with pytest.raises(ValueError) as excinfo:
        _ = regroup(data2, 2, check_first=True).collect()
    assert 'first item was not not length 2' in str(excinfo.value)
