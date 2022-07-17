<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# AWS Integration Tests

The AWS integration test suite runs tests against an AWS account to verify behaviors in a real AWS cloud environment.

## Setup

The test suite uses the [default AWS credential chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html) to detect the credentials used to run tests.
For example, the AWS Tests CI workflow uses the credentials set in environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

In addition, the test suite requires setting the following environment variables:
- `AWS_REGION`: AWS region to run tests, e.g. `us-east-1`
- `AWS_CROSS_REGION`: AWS region to run any cross-region operations, e.g. `us-west-2`
- `AWS_TEST_ACCOUNT_ID`: AWS account ID to run tests, e.g. `123456789012`
- `AWS_TEST_BUCKET`: S3 bucket to run tests, e.g. `my-bucket`
- `AWS_TEST_CROSS_REGION_BUCKET`: S3 bucket to run any cross-region operations, e.g. `my-cross-region-bucket`

Here is an example script to execute integration tests in terminal:

```shell
# set environment variables
export AWS_ACCESS_KEY_ID=ABCDEFG1234567890
export AWS_SECRET_ACCESS_KEY=qwertyuiopasdfghjklzxcvbnm
export AWS_REGION=us-east-1
export AWS_CROSS_REGION=us-west-2
export AWS_TEST_ACCOUNT_ID=123456789012
export AWS_TEST_BUCKET=my-bucket
export AWS_TEST_CROSS_REGION_BUCKET=my-cross-region-bucket

# run all tests
./gradlew :iceberg-aws:integrationTest

# run a single test suite
./gradlew :iceberg-aws:integrationTest --tests "org.apache.iceberg.aws.s3.TestS3FileIOIntegration"

# run a single test
./gradlew :iceberg-aws:integrationTest --tests "org.apache.iceberg.aws.s3.TestS3FileIOIntegration.testServerSideKmsEncryption"
```

## Permissions

The following JSON payload describes the minimum set of permissions required to execute tests.
Developers interested in running the test suite against a personal AWS account can use this for reference.
The AWS Tests CI workflow uses the same set of permissions.
To add new permissions in CI, please contact [dev@iceberg.apache.org](mailto:dev@iceberg.apache.org).

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3BucketPermissions",
      "Effect": "Allow",
      "Action": [
        "s3:GetAccessPoint",
        "s3:CreateAccessPoint",
        "s3:DeleteAccessPoint",
        "s3:ListAccessPoints",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::iceberg-aws-ci-220427471354-us-east-1",
        "arn:aws:s3:::iceberg-aws-ci-220427471354-us-west-2"
      ]
    },
    {
      "Sid": "S3ObjectPermissions",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObjectAcl",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::iceberg-aws-ci-220427471354-us-east-1/*",
        "arn:aws:s3:::iceberg-aws-ci-220427471354-us-west-2/*"
      ]
    },
    {
      "Sid": "GluePermissions",
      "Effect": "Allow",
      "Action": [
        "glue:UpdateDatabase",
        "glue:DeleteDatabase",
        "glue:CreateTable",
        "glue:GetTables",
        "glue:GetTableVersions",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetDatabase",
        "glue:GetTableVersion",
        "glue:CreateDatabase"
      ],
      "Resource": [
        "arn:aws:glue:us-east-1:220427471354:catalog",
        "arn:aws:glue:us-east-1:220427471354:database/iceberg_aws_ci_*",
        "arn:aws:glue:us-east-1:220427471354:table/iceberg_aws_ci_*/*",
        "arn:aws:glue:us-east-1:220427471354:userDefinedFunction/iceberg_aws_ci_*/*"
      ]
    },
    {
      "Sid": "STSPermissions",
      "Effect": "Allow",
      "Action": [
        "sts:TagSession",
        "sts:AssumeRole"
      ],
      "Resource": [
        "arn:aws:iam::220427471354:role/iceberg-aws-ci-*"
      ]
    },
    {
      "Sid": "KMSKeyCreationPermissions",
      "Effect": "Allow",
      "Action": [
        "kms:CreateKey"
      ],
      "Resource": "*"
    },
    {
      "Sid": "KMSAliasPermissions",
      "Effect": "Allow",
      "Action": [
        "kms:CreateAlias"
      ],
      "Resource": [
        "arn:aws:kms:us-east-1:220427471354:key/*",
        "arn:aws:kms:us-east-1:220427471354:alias/iceberg-aws-ci-*"
      ]
    },
    {
      "Sid": "KMSKeyUsagePermissions",
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:GenerateDataKey",
        "kms:ScheduleKeyDeletion"
      ],
      "Resource": "arn:aws:kms:us-east-1:220427471354:key/*",
      "Condition": {
        "ForAnyValue:StringLike": {
          "kms:ResourceAliases": "alias/iceberg-aws-ci-*"
        }
      }
    },
    {
      "Sid": "IAMPermissions",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:DeleteRole",
        "iam:PutRolePolicy",
        "iam:DeleteRolePolicy"
      ],
      "Resource": [
        "arn:aws:iam::220427471354:role/iceberg-aws-ci-*"
      ]
    },
    {
      "Sid": "DynamoDBPermissions",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:CreateTable",
        "dynamodb:DescribeTable",
        "dynamodb:GetItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:us-east-1:220427471354:table/IcebergAwsCI_*"
      ]
    }
  ]
}
```
