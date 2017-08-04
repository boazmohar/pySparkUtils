from builtins import str
from pySparkUtils.utils import change
import pytest
pytestmark = pytest.mark.usefixtures("eng")


def test_ip_wait(eng):
    with pytest.raises(RuntimeError) as ex:
        _ = change(sc=eng, master='local[2]', fail_on_timeout=True, wait='ips', min_ips=10, timeout=4)
    assert 'Time out' in str(ex.value)


def test_cores_wait(eng):
    eng.stop()
    new_sc = change(sc=None, master='local[2]', fail_on_timeout=False, wait='cores', min_cores=2)
    assert new_sc.defaultParallelism == 2
    new_sc.stop()


def test_cores_wait2(eng):
    old_default = eng.defaultParallelism
    new_sc = change(sc=eng, master=None, fail_on_timeout=False, wait='cores', min_cores=None)
    assert new_sc.defaultParallelism == old_default
    new_sc.stop()


def test_cores_wait3(eng):
    with pytest.raises(RuntimeError) as ex:
        _ = change(sc=eng, master='local[2]', fail_on_timeout=True, wait='cores', min_cores=3, timeout=4)
    assert 'Time out' in str(ex.value)


def test_cores_wait4(eng):
    eng.stop()
    new_sc = change(sc=None, master='local', fail_on_timeout=False, wait='cores', min_cores=None)
    assert new_sc.defaultParallelism >= 1
    new_sc.stop()


def test_no_input(eng):
    with pytest.raises(ValueError) as ex:
        change(sc=None, master=None)
    assert 'Both master and sc are None' in str(ex.value)
    with pytest.raises(ValueError) as ex:
        change(sc=eng, master=None, wait='error')
    assert 'wait should be' in str(ex.value)


def test_cores(eng):
    n_cores = eng.defaultParallelism
    new_sc = change(sc=eng)
    assert new_sc.defaultParallelism == n_cores
    new_sc.stop()


def test_local(eng):
    eng.stop()
    new_sc = change(sc=None, master='local[2]', fail_on_timeout=False)
    assert new_sc.defaultParallelism == 2
    new_sc.stop()


def test_args(eng):
    old_conf = eng.getConf()
    old_value = old_conf.get('spark.rpc.message.maxSize')
    if old_value is None:
        new_value = u'250'
    else:
        new_value = str(int(old_value+1))
    new_sc = change(sc=eng, spark_rpc_message_maxSize=new_value)
    new_conf = new_sc.getConf()
    assert new_conf.get('spark.rpc.message.maxSize') == new_value
    new_sc.stop()
