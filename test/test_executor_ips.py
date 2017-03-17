from pySparkUtils.utils import executor_ips
from mock import Mock
import pytest
import os
pytestmark = pytest.mark.usefixtures("eng")

resources = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         'Resources')


def test_url(eng, mocker):
    mock_urlopen = mocker.patch('pySparkUtils.utils.urllib2.urlopen')
    mock_response = Mock()
    res1 = open(os.path.join(resources, 'json1.txt'), 'r').read()
    res2 = open(os.path.join(resources, 'json40.txt'), 'r').read()
    mock_response.read.side_effect = [res1, res2]
    mock_urlopen.return_value = mock_response
    res = executor_ips(eng)
    assert len(res) == 1
    res = executor_ips(eng)
    assert len(res) == 41
