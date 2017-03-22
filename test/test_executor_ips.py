from pySparkUtils.utils import executor_ips
import pytest
pytestmark = pytest.mark.usefixtures("eng")


def test_url(eng, mocker):
    res = executor_ips(eng)
    assert len(res) == 1
