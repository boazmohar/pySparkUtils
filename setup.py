from setuptools import setup, find_packages
from codecs import open
from os import path
here = path.abspath(path.dirname(__file__))


with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requires = f.read().splitlines()

with open(path.join(here, 'requirements-dev.txt'), encoding='utf-8') as f:
    requires_dev = f.read().splitlines()

setup(
    name='pySparkUtils',
    packages=find_packages(exclude=['dist', 'docs', 'test']),
    version='0.2.4',
    description="A collection of utilities for handling pySpark's SparkContext",
    long_description=long_description,
    author='Boaz Mohar',
    author_email='boazmohar@gmail.com',
    license='MIT',
    url='https://github.com/boazmohar/pySparkUtils',
    download_url='https://github.com/boazmohar/pySparkUtils/archive/0.2.4.tar.gz',
    keywords=['spark', 'pyspark', ],
    classifiers=['Development Status :: 3 - Alpha',
                 'License :: OSI Approved :: MIT License',
                 'Programming Language :: Python :: 2',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3',
                 'Programming Language :: Python :: 3.5',
                 ],
    install_requires=requires,
    extras_require={
        'dev': requires_dev,
    },
)
