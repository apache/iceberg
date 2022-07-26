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


class NoSuchTableError(Exception):
    """Raised when a referenced table is not found"""


class NoSuchNamespaceError(Exception):
    """Raised when a referenced name-space is not found"""


class NamespaceNotEmptyError(Exception):
    """Raised when a name-space being dropped is not empty"""


class AlreadyExistsError(Exception):
    """Raised when a table or name-space being created already exists in the catalog"""


class ValidationError(Exception):
    """Raises when there is an issue with the schema"""
