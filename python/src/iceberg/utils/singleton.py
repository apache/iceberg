#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
"""
This is a singleton metaclass that can be used to cache and re-use existing objects

In the Iceberg codebase we have a lot of objects that are stateless (for example Types such as StringType,
BooleanType etc). FixedTypes have arguments (eg. Fixed[22]) that we also make part of the key when caching
the newly created object.

The Singleton uses a metaclass which essentially defines a new type. When the Type gets created, it will first
evaluate the `__call__` method with all the arguments. If we already initialized a class earlier, we'll just
return it.

More information on metaclasses: https://docs.python.org/3/reference/datamodel.html#metaclasses

"""
from abc import ABCMeta
from typing import ClassVar, Dict


class Singleton(ABCMeta):
    _instances: ClassVar[Dict] = {}

    def __call__(cls, *args, **kwargs):
        key = (cls, args, tuple(sorted(kwargs.items())))
        if key not in cls._instances:
            cls._instances[key] = super().__call__(*args, **kwargs)
        return cls._instances[key]
