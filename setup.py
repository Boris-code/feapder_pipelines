# -*- coding: utf-8 -*-
"""
Created on 2020/4/22 10:45 PM
---------
@summary:
---------
@author: Boris
@email: boris_liu@foxmail.com
"""

from os.path import dirname, join
from sys import version_info

import setuptools

if version_info < (3, 6, 0):
    raise SystemExit("Sorry! feapder_pipelines requires python 3.6.0 or later.")

with open(join(dirname(__file__), "feapder_pipelines/VERSION"), "rb") as f:
    version = f.read().decode("ascii").strip()

with open("README.md", "r") as fh:
    long_description = fh.read()

packages = setuptools.find_packages()
packages.extend(
    [
        "feapder_pipelines",
    ]
)

requires = [
    "feapder",
]

extras_require = {"pgsql": ["psycopg2-binary>=2.9.2"]}

setuptools.setup(
    name="feapder_pipelines",
    version=version,
    author="Boris",
    license="MIT",
    author_email="feapder@qq.com",
    python_requires=">=3.6",
    description="feapder pipelines extension",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=requires,
    extras_require=extras_require,
    url="https://github.com/Boris-code/feapder_pipelines.git",
    packages=packages,
    include_package_data=True,
    classifiers=["Programming Language :: Python :: 3"],
)
