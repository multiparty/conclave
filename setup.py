from setuptools import setup

setup(
    name             = 'conclave',
    version          = '0.0.0.1',
    packages         = ['conclave',],
    install_requires = ['pystache', 'swiftclient', 'keystoneauth1'],
    license          = 'MIT',
    url              = 'https://github.com/multiparty/conclave',
    description      = 'Infrastructure for defining and running large data workflows against multiple backends.',
    long_description = open('README.rst').read(),
    test_suite       = 'nose.collector',
    tests_require    = ['nose'],
)
