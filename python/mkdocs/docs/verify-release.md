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

# Verifying a release

Each Apache PyIceberg release is validated by the community by holding a vote. A community release manager will prepare a release candidate and call a vote on the Iceberg dev list. To validate the release candidate, community members will test it out in their downstream projects and environments.

In addition to testing in downstream projects, community members also check the release’s signatures, checksums, and license documentation.

## Validating a release candidate

Release announcements include links to the following:

- A source tarball
- A signature (.asc)
- A checksum (.sha512)
- KEYS file
- GitHub change comparison

After downloading the source tarball, signature, checksum, and KEYS file, here are instructions on how to verify signatures, checksums, and documentation.

## Verifying signatures

First, import the keys.

```sh
curl https://dist.apache.org/repos/dist/dev/iceberg/KEYS -o KEYS
gpg --import KEYS
```

Next, verify the `.asc` file.

```sh
gpg --verify pyiceberg-0.4.0-py3-none-any.whl.asc pyiceberg-0.4.0-py3-none-any.whl
```

## Verifying checksums

```sh
shasum -a 512 --check pyiceberg-0.4.0-py3-none-any.whl.sha512
```

## Verifying License Documentation

```sh
tar xzf pyiceberg-0.4.0.tar.gz
cd pyiceberg-0.4.0
```

Run RAT checks to validate license header:

```
./dev/check-license
```

## Testing

This section explains how to run the tests of the source distribution.

<!-- prettier-ignore-start -->

!!! note "Clean environment"
    To make sure that your environment is fresh is to run the tests in a new Docker container:
    `docker run -t -i -v $(pwd):/pyiceberg/ python:3.9 bash`. And change directory: `cd /pyiceberg/`.

<!-- prettier-ignore-end -->

First step is to install the package:

```sh
make install
```

And then run the tests:

```sh
make test
```

To run the full integration tests:

```sh
make test-s3
```

This will include a Minio S3 container being spun up.

# Cast the vote

Votes are cast by replying to the release candidate announcement email on the dev mailing list with either `+1`, `0`, or `-1`. For example :

> \[ \] +1 Release this as PyIceberg 0.3.0 \[ \] +0 \[ \] -1 Do not release this because…

In addition to your vote, it’s customary to specify if your vote is binding or non-binding. Only members of the Project Management Committee have formally binding votes. If you’re unsure, you can specify that your vote is non-binding. To read more about voting in the Apache framework, checkout the [Voting](https://www.apache.org/foundation/voting.html) information page on the Apache foundation’s website.
