from pySparkUtils.utils import fallback, change
import pytest
pytestmark = pytest.mark.usefixtures("eng")


def test_fallback(eng):
    target_cores = eng.defaultParallelism
    @fallback
    def test_func(sc_inner):
        new_sc = change(sc_inner)
        raise ValueError(new_sc.defaultParallelism)

    sc = test_func(eng)
    assert sc.defaultParallelism == target_cores
    sc.stop()