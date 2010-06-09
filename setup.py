from setuptools import setup

setup(
    name='gruneberg',
    version='0.1',
    description='A python library to emit ganglia metrics based on various data sources',
    
    packages=['gruneberg'],
    package_dir={'gruneberg': 'gruneberg'}
    package_data = { 'gruneberg': ['*.txt', '*.rst'] },
    install_requires=['thrift'],
    
    author='Xavier Stevens',
    author_email='xstevens@mozilla.com',
    license='MIT',
    url='http://github.com/xstevens/gruneberg')
