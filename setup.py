from distutils.core import setup
setup(
  name = 'pySparkUtils',
  packages = ['pySparkUtils'], # this must be the same as the name above
  version = '0.1.2',
  description = "A collection of utilities for handling pySpark's SparkContext",
  author = 'Boaz Mohar',
  author_email = 'boazmohar@gmail.com',
  url = 'https://github.com/boazmohar/pySparkUtils', # use the URL to the github repo
  download_url = 'https://github.com/boazmohar/pySparkUtils/archive/0.1.tar.gz', # I'll explain this in a second
  keywords = ['spark', 'pyspark',],
  classifiers = [],
  install_requires=open('requirements.txt').read().split('\n'),
)
