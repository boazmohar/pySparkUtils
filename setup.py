from distutils.core import setup

setup(
    name='pySparkUtils',
    packages=['pySparkUtils'],
    version='0.1.5',
    description="A collection of utilities for handling pySpark's SparkContext",
    author='Boaz Mohar',
    author_email='boazmohar@gmail.com',
    url='https://github.com/boazmohar/pySparkUtils',
    download_url='https://github.com/boazmohar/pySparkUtils/archive/0.1.4.tar.gz',
    keywords=['spark', 'pyspark', ],
    classifiers=[],
    install_requires=open('requirements.txt').read().split('\n'),
)
