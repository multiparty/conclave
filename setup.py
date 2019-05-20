from setuptools import setup, find_packages

setup(
    name             = 'conclave',
    version          = '0.0.0.1',
    packages         = find_packages(),
    license          = 'MIT',
    url              = 'https://github.com/cici-conclave/conclave',
    description      = 'Infrastructure for defining and running large data workflows against multiple backends.',
    long_description = open('README.md').read(),
    test_suite       = 'nose.collector',
    tests_require    = ['nose'],
)
