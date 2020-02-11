[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fboazmohar%2FpySparkUtils.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fboazmohar%2FpySparkUtils?ref=badge_shield)

pySparkUtils
============

|Build Status| |PyPI version| |Updates| |Cover|


A collection of utilities for handling pySpark SparkContext

install
-------

.. code:: bash

    pip install pySparkUtils

support
-------

Tested on Spark 2.2 with python 2.7, 3.5 and 3.6

documentation
-------------

`API Documentation`_

contributing
------------

Fell free to create issues or PRs

release
-------

.. code:: bash

    py.test -s -v --cov=./pySparkUtils --pep8
    bumpversion patch
    /docs/make html
    python setup.py sdist
    twine upload dist/...

.. _API Documentation: https://boazmohar.github.io/pySparkUtils/pySparkUtils.html#module-pySparkUtils.utils

.. |Updates| image:: https://pyup.io/repos/github/boazmohar/pySparkUtils/shield.svg
   :target: https://pyup.io/repos/github/boazmohar/pySparkUtils/
.. |Build Status| image:: https://travis-ci.org/boazmohar/pySparkUtils.svg?branch=master
   :target: https://travis-ci.org/boazmohar/pySparkUtils
.. |PyPI version| image:: https://badge.fury.io/py/pySparkUtils.svg
   :target: https://badge.fury.io/py/pySparkUtils
.. |Cover| image:: https://coveralls.io/repos/github/boazmohar/pySparkUtils/badge.svg?branch=master
   :target: https://coveralls.io/github/boazmohar/pySparkUtils?branch=master


## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fboazmohar%2FpySparkUtils.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fboazmohar%2FpySparkUtils?ref=badge_large)