<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Voting on an Iceberg Release

Before a new version of Apache Iceberg is released, a release candidate will be prepared and a vote
will occur. Announcements will be made on the Iceberg [developer's mailing list](http://127.0.0.1:8000/#community/#mailing-lists)
and will include links to the following:

- A source tarball
- A signature (.asc)
- A checksum (.sha512)
- Key files
- GitHub change comparison

After downloading the source tarball, signature, checksum, and key files, follow
these steps to participate in the voting process.

Import the keys.
```bash
gpg --import ${PATH_TO_KEY_FILES}
```

Verify the signature.
```bash
gpg --verify apache-iceberg-${VERSION}.tar.gz.asc
```

Verify the checksum.
```bash
sha512sum -c apache-iceberg-${VERSION}.tar.gz.sha512
```

Untar the archive and change into the source directory.
```bash
tar xzf apache-iceberg-${VERSION}.tar.gz
cd apache-iceberg-${VERSION}
```

Run RAT checks to validate license headers.
```bash
dev/check-license
```

Build and test the project using Java 8.
```bash
./gradlew build
```

Votes are cast by replying to the release candidate announcement email on the dev mailing list
with either +1, 0, or -1.

> [ ] +1 Release this as Apache Iceberg ${VERSION}  
[ ] +0  
[ ] -1 Do not release this because...  

In addition to your vote, it's customary to specify if your vote is binding or non-binding. Only members
of the Project Management Committee have formally binding votes. If you're unsure, you can specify that your
vote is non-binding. To read more about voting in the Apache framework, checkout the
[Voting](https://www.apache.org/foundation/voting.html) information page on the Apache foundation's website.