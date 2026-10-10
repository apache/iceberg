---
title: "Security"
---
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

# Reporting Security Issues

The Apache Iceberg Project uses the standard process outlined by the [Apache
Security Team](https://www.apache.org/security/) for reporting vulnerabilities.
Note that vulnerabilities should not be publicly disclosed until the project has
responded.

To report a possible security vulnerability, please email <a href="mailto:security@iceberg.apache.org">security@iceberg.apache.org</a>.

# Security Model

Apache Iceberg is a table format and a set of libraries and integrations used inside larger systems such as catalogs, query engines, and services.
In most deployments, the primary trust and authorization boundaries are enforced by the surrounding catalog, engine, service, operator configuration, and storage-level authorization rather than by Iceberg alone.

Iceberg security issues generally include secret or credential disclosure to a newly reachable audience, and other cases where Iceberg itself creates a new unauthorized capability rather than merely reflecting the trust decisions of a catalog, engine, or operator.

Many other issues may still be valid bugs, but are not normally considered security vulnerabilities in Iceberg.
This includes robustness issues such as malformed-input crashes or memory exhaustion, as well as issues that require a malicious catalog, metastore, or other external service.

Potential vulnerabilities that fall within this security model should be reported privately using the process above.
Other bugs and hardening issues should be reported through the public issue tracker.

For a more detailed threat model used for agent-assisted triage and scanner calibration, see the [Apache Iceberg Security Threat Model](https://github.com/apache/iceberg/blob/main/SECURITY-THREAT-MODEL.md).

# Verifying Signed Releases

Please refer to the instructions on the [Release Verification](https://www.apache.org/info/verification.html) page.
