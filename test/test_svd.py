from sklearn.datasets import make_low_rank_matrix
import pytest
from thunder.images import fromarray
from pySparkUtils.SVD import getSVD
pytestmark = pytest.mark.usefixtures("eng")


def test_svd(eng):
    x_local = make_low_rank_matrix(n_samples=10, n_features=50, random_state=0)
    x = fromarray(x_local.reshape(10, 10, 5), engine=eng)

    from sklearn.utils.extmath import randomized_svd
    u1, s1, v1 = randomized_svd(x_local, n_components=2,  random_state=0)

    u2, v2, s2 = getSVD(x, k=2, getComponents=True, getS=True)
    assert u1.shape == u2.shape
    assert s1.shape == s2.shape
    assert v1.shape == (2, 50)
    assert v2.shape == (2, 10, 5)