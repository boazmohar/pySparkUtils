from builtins import str
from pySparkUtils.utils import change
import pytest
pytestmark = pytest.mark.usefixtures("eng")

def test_no_input():
    with pytest.raises(ValueError) as excinfo:
        change(sc=None, master=None)
    assert 'Both master and sc are None' in str(excinfo.value)

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
