# Encoding: utf-8
"""
originally: http://stackoverflow.com/questions/33428589/
pyspark-and-pca-how-can-i-extract-the-eigenvectors-of-this-pca-how-can-i-calcu/33500704#33500704
from https://blog.dominodatalab.com/pca-on-very-large-neuroimaging-datasets-using-pyspark/

"""

from pyspark.mllib.common import JavaModelWrapper
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.linalg import _convert_to_vector, DenseMatrix
from scipy.stats.mstats import zscore
import numpy as np
from pySparkUtils.utils import thunder_decorator


class RowMatrix_new(RowMatrix):
    def multiply(self, matrix):
        """
        Multiplies the given RowMatrix with another matrix.
        :param matrix: Matrix to multiply with.
        :returns: RowMatrix example:
        rm = RowMatrix(sc.parallelize([[0, 1], [2, 3]]))
        rm.multiply(DenseMatrix(2, 2, [0, 2, 1, 3])).rows.collect()
        [DenseVector([2.0, 3.0]), DenseVector([6.0, 11.0])]
        """
        if not isinstance(matrix, DenseMatrix):
            raise ValueError("Only multiplication with DenseMatrix "
                             "is supported.")
        j_model = self._java_matrix_wrapper.call("multiply", matrix)
        return RowMatrix_new(j_model)


class SVD(JavaModelWrapper):
    """Wrapper around the SVD scala case class"""

    @property
    def U(self):
        """
        Returns a RowMatrix whose columns are the left singular vectors of the SVD if computeU was set to be True.
        """
        u = self.call("U")
        if u is not None:
            return RowMatrix(u)

    @property
    def s(self):
        """Returns a DenseVector with singular values in descending order."""
        return self.call("s")

    @property
    def V(self):
        """ Returns a DenseMatrix whose columns are the right singular vectors of the SVD."""
        return self.call("V")


def compute_svd(row_matrix, k, compute_u=False, r_cond=1e-9):
    """

    Computes the singular value decomposition of the RowMatrix.
    :param row_matrix:
    The given row matrix A of dimension (m X n) is decomposed into U * s * V'T where
    * s: DenseVector consisting of square root of the eigenvalues (singular values) in descending order.
    * U: (m X k) (left singular vectors) is a RowMatrix whose columns are the eigenvectors of (A X A')
    * v: (n X k) (right singular vectors) is a Matrix whose columns are the eigenvectors of (A' X A)
    :param k: number of singular values to keep. We might return less than k if there are numerically zero
     singular values.
    :param compute_u: Whether of not to compute U. If set to be True, then U is computed by A * V * sigma^-1
    :param r_cond: the reciprocal condition number. All singular values smaller than rCond * sigma(0) are treated
     as zero, where sigma(0) is the largest singular value.
    :returns: SVD object
    """
    java_model = row_matrix._java_matrix_wrapper.call("computeSVD", int(k), compute_u, float(r_cond))
    return SVD(java_model)


def getSVD(data, k, getComponents=False, getS=False, normalization='mean'):
    """ Wrapper for computeSVD that will normalize and handle a Thunder Images object

    :param data: Thunder Images object
    :param k: number of components to keep
    :param getComponents: will return the components if true, otherwise will return None

    :returns: projections, components, s
    """
    if normalization == 'nanmean':
        data2 = data.tordd().sortByKey().values().map(lambda x: _convert_to_vector(x.flatten() - np.nanmean(x)))
    elif normalization == 'mean':
        data2 = data.tordd().sortByKey().values().map(lambda x: _convert_to_vector(x.flatten() - x.mean()))
    elif normalization is 'zscore':
        data2 = data.tordd().sortByKey().values().map(lambda x: _convert_to_vector(zscore(x.flatten())))
    elif normalization is None:
        data2 = data.tordd().sortByKey().values().map(lambda x: _convert_to_vector(x.flatten()))
    else:
        raise ValueError('Normalization should be one of: mean, nanmean, zscore, None. Got: %s' % normalization)

    mat = RowMatrix(data2)
    mat.rows.cache()
    mat.rows.count()
    svd = compute_svd(row_matrix=mat, k=k, compute_u=False)
    if getComponents:
        components = svd.call("V").toArray()
        components = components.transpose(1, 0).reshape((k,) + data.shape[1:])
    else:
        components = None
    projection = np.array(RowMatrix_new(data2).multiply(svd.call("V")).rows.collect())
    if getS:
        s = svd.call("s").toArray()
    else:
        s = None
    return projection, components, s
