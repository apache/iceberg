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

import time
from urllib.parse import urlparse

import boto3

from .local_filesystem import LocalFileSystem
from .s3_filesystem_wrapper import S3FileSystemWrapper


def get_fs(path, conf, local_only=False):
    if local_only:
        return LocalFileSystem.get_instance()
    else:
        parsed_path = urlparse(path)

        if parsed_path.scheme in ["", "file"]:
            return LocalFileSystem.get_instance()
        elif parsed_path.scheme in ["s3", "s3n"]:
            if conf.get("hive.aws_iam_role") is not None:
                key, secret, token = get_sts_session_keys(conf.get("hive.aws_iam_role"))
                return S3FileSystemWrapper(key=key, secret=secret, token=token)
            return S3FileSystemWrapper()
        elif parsed_path.scheme in ["hdfs"]:
            raise RuntimeError("Hadoop FS not implemented")

    raise RuntimeError("No filesystem found for this location: %s" % path)


def get_sts_session_keys(role_arn):
    client = boto3.client('sts')
    response = client.assume_role(
        RoleArn=role_arn,
        RoleSessionName='iceberg_python_client_{}'.format(int(time.time())))
    return (response["Credentials"]["AccessKeyId"],
            response["Credentials"]["SecretAccessKey"],
            response["Credentials"]["SessionToken"])
