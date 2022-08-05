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

from typing import Optional, Union

from pydantic import Field

from pyiceberg.catalog.base import Identifier
from pyiceberg.table.metadata import TableMetadataV1, TableMetadataV2
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel


class Table(IcebergBaseModel):
    identifier: Identifier = Field()
    metadata_location: str = Field()
    metadata: Optional[Union[TableMetadataV1, TableMetadataV2]] = Field()
