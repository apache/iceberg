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

import re

from .bucket import Bucket
from .dates import Dates
from .identity import Identity
from .timestamps import Timestamps
from .truncate import Truncate
from .unknown_transform import UnknownTransform
from .void_transform import VoidTransform
from ..types import (TypeID)


"""
 Factory methods for transforms.
 <p>
 Most users should create transforms using a
 {@link PartitionSpec.Builder#builderFor(Schema)} partition spec builder}.

 @see PartitionSpec#builderFor(Schema) The partition spec builder.
"""


class Transforms(object):
    HAS_WIDTH = re.compile("(\\w+)\\[(\\d+)\\]")

    def __init__(self):
        pass

    @staticmethod
    def from_string(type_var, transform):
        match = Transforms.HAS_WIDTH.match(transform)

        if match is not None:
            name = match.group(1)
            w = int(match.group(2))
            if name.lower() == "truncate":
                return Truncate.get(type_var, w)
            elif name.lower() == "bucket":
                return Bucket.get(type_var, w)

        if transform.lower() == "identity":
            return Identity.get(type_var)
        elif type_var.type_id == TypeID.TIMESTAMP:
            return Timestamps(transform.lower(), transform.lower())
        elif type_var.type_id == TypeID.DATE:
            return Dates(transform.lower(), transform.lower())

        if transform.lower() == "void":
            return VoidTransform.get()

        return UnknownTransform(type_var, transform)

    @staticmethod
    def identity(type_var):
        return Identity.get(type_var)

    @staticmethod
    def year(type_var):
        if type_var.type_id == TypeID.DATE:
            return Dates("year", "year")
        elif type_var.type_id == TypeID.TIMESTAMP:
            return Timestamps("year", "year")
        else:
            raise RuntimeError("Cannot partition type %s by year" % type_var)

    @staticmethod
    def month(type_var):
        if type_var.type_id == TypeID.DATE:
            return Dates("month", "month")
        elif type_var.type_id == TypeID.TIMESTAMP:
            return Timestamps("month", "month")
        else:
            raise RuntimeError("Cannot partition type %s by month" % type_var)

    @staticmethod
    def day(type_var):
        if type_var.type_id == TypeID.DATE:
            return Dates("day", "day")
        elif type_var.type_id == TypeID.TIMESTAMP:
            return Timestamps("day", "day")
        else:
            raise RuntimeError("Cannot partition type %s by day" % type_var)

    @staticmethod
    def hour(type_var):
        if type_var.type_id == TypeID.DATE:
            return Dates("hour", "hour")
        elif type_var.type_id == TypeID.TIMESTAMP:
            return Timestamps("hour", "hour")
        else:
            raise RuntimeError("Cannot partition type %s by hour" % type_var)

    @staticmethod
    def bucket(type_var, num_buckets):
        return Bucket.get(type_var, num_buckets)

    @staticmethod
    def truncate(type_var, width):
        return Truncate.get(type_var, width)

    @staticmethod
    def always_null():
        return VoidTransform.get()
