# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from setuptools import setup

setup(
    name='iceberg',
    maintainer='Apache Iceberg Devs',
    author_email='dev@iceberg.apache.org',
    description='Iceberg is a new table format for storing large, slow-moving tabular data',
    keywords='iceberg',
    url='https://github.com/apache/iceberg/blob/master/README.md',
    python_requires='>=3.7',
    install_requires=['botocore',
                      'boto3',
                      'fastavro>=1.3.2<1.4.0',
                      'hmsclient==0.1.1',
                      'mmh3',
                      'pyparsing>=2.4.7<2.5.0',
                      'python-dateutil',
                      'pytz',
                      'requests',
                      'retrying',
                      'pandas',
                      'pyarrow>=3.0.0,<=4.0.1'
                      ],
    extras_require={
        "dev": [
            "tox-travis==0.12",
            "virtualenv<20.0.0",
        ],
    },
    setup_requires=['setupmeta'],
    license="Apache License 2.0",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
