#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

_REQUIRES = ["grpcio>=1.33.2,<2.0.0", "protobuf>=3.20.0,<5.0.0"]

setup(
    name="etcd3-client",
    version="0.90.0",
    description="Python client for the etcd3",
    long_description="",
    url="https://github.com/gooddata/etcd3-client",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    install_requires=_REQUIRES,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords="etcd3 client",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Typing :: Typed",
    ],
)
