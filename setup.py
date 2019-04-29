#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-s3",
    version="0.1.0",
    description="Singer.io target loading data to an S3 bucket",
    author="dcereijodo",
    url="https://github.com/dcereijodo/target-s3",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_s3"],
    install_requires=[
        "boto3>=1.9.97",
    ],
    entry_points="""
    [console_scripts]
    target-s3=target_s3:main
    """,
    packages=["target_s3"],
    package_data = {},
    include_package_data=True
)
