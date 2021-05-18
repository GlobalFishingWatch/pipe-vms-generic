#!/usr/bin/env python

"""
Setup script for pipe-vms-generic
"""

from setuptools import find_packages
from setuptools import setup

import codecs

package = __import__('pipe_vms_generic')


with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()


setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    install_requires=[],
    license="Apache 2.0",
    long_description=readme,
    name='pipe-vms-generic',
    packages=find_packages(exclude=['test*.*', 'tests']),
    url=package.__source__,
    version=package.__version__,
    zip_safe=True
)
