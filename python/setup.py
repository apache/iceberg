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

import sys

from setuptools import setup

DEPENDENCIES = ['botocore==1.20.14',
                'boto3==1.17.14',
                'fastavro==1.3.2',
                'hmsclient==0.1.1',
                'mmh3==3.0.0',
                'pyparsing==2.4.7',
                'python-dateutil==2.8.1',
                'pytz==2021.1',
                'requests==2.25.1',
                'retrying==1.3.3',
                'pyarrow==2.0.0'
                ]

# pandas doesn't get built for 3.6 anymore, we should consider deprecating 3.6
if sys.version_info.minor == 6 and sys.version_info.major == 3:
    DEPENDENCIES.append('pandas==1.1.5')
else:
    DEPENDENCIES.append('pandas==1.2.2')

setup(
    name='iceberg',
    maintainer='Apache Iceberg Devs',
    author_email='dev@iceberg.apache.org',
    description='Iceberg is a new table format for storing large, slow-moving tabular data',
    keywords='iceberg',
    url='https://github.com/apache/iceberg/blob/master/README.md',
    python_requires='>=3.6',
    install_requires=DEPENDENCIES,
    setup_requires=['setupmeta'],
    license="Apache License 2.0",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
