from pySparkUtils.utils import fallback, change
import pytest

pytestmark = pytest.mark.usefixtures("eng")


def test_fail(eng):
    target_cores = eng.defaultParallelism

    @fallback
    def test_func(sc_inner):
        new_sc = change(sc_inner)
        raise ValueError(new_sc.defaultParallelism)

    sc = test_func(eng)
    assert sc.defaultParallelism == target_cores
    sc.stop()


def test_pass(eng):
    target_cores = eng.defaultParallelism

    @fallback
    def test_func(sc_inner):
        new_sc = change(sc_inner)
        return new_sc

    sc = test_func(eng)
    assert sc.defaultParallelism == target_cores
    sc.stop()


def test_no_sc():
    @fallback
    def test_func(arg1):
        raise ValueError(arg1)

    result = test_func(10)
    assert result is None


def test_stopped_sc(eng):
    @fallback
    def test_func(sc_inner):
        sc_inner.stop()
        raise ValueError(10)

    result = test_func(eng)
    assert result is None
