"""
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""

from setuptools import setup


setup(
    name='iceberg',
    versioning='build-id',
    author='bigdatacompute',
    author_email='BigDataCompute@netflix.com',
    description='Iceberg is a new table format for storing large, slow-moving tabular data',
    keywords='iceberg',
    url='https://github.com/Netflix/iceberg',
    python_requires='>=2.7',
    install_requires=['enum34;python_version<"3.4"',
                      'fastavro',
                      'mmh3',
                      'nose',
                      'pyarrow',
                      'python-dateutil',
                      'pytz',
                      'retrying',
                      'six']
)
