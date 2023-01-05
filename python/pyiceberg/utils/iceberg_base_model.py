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
from functools import cached_property
from typing import (
    Any,
    Dict,
    Optional,
    Set,
)

from pydantic import BaseModel


class IcebergBaseModel(BaseModel):
    """
    This class extends the Pydantic BaseModel to set default values by overriding them.

    This is because we always want to set by_alias to True. In Python, the dash can't
    be used in variable names, and this is used throughout the Iceberg spec.

    The same goes for exclude_none, if a field is None we want to omit it from
    serialization, for example, the doc attribute on the NestedField object.
    Default non-null values will be serialized.

    This is recommended by Pydantic:
    https://pydantic-docs.helpmanual.io/usage/model_config/#change-behaviour-globally
    """

    class Config:
        keep_untouched = (cached_property,)
        allow_population_by_field_name = True
        frozen = True

    def _exclude_private_properties(self, exclude: Optional[Set[str]] = None) -> Set[str]:
        # A small trick to exclude private properties. Properties are serialized by pydantic,
        # regardless if they start with an underscore.
        # This will look at the dict, and find the fields and exclude them
        return set.union(
            {field for field in self.__dict__ if field.startswith("_") and not field == "__root__"}, exclude or set()
        )

    def dict(self, exclude_none: bool = True, exclude: Optional[Set[str]] = None, **kwargs: Any) -> Dict[str, Any]:
        return super().dict(exclude_none=exclude_none, exclude=self._exclude_private_properties(exclude), **kwargs)

    def json(self, exclude_none: bool = True, exclude: Optional[Set[str]] = None, by_alias: bool = True, **kwargs: Any) -> str:
        return super().json(
            exclude_none=exclude_none, exclude=self._exclude_private_properties(exclude), by_alias=by_alias, **kwargs
        )
