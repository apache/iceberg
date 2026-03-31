#!/usr/bin/env python3
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

import json
import sys
import urllib.error
import urllib.parse
import urllib.request

POLARIS_URL = "http://localhost:8181/api/management/v1"
POLARIS_TOKEN_URL = "http://localhost:8181/api/catalog/v1/oauth/tokens"

def request(url, method="GET", data=None, headers=None):
    if headers is None:
        headers = {}
    
    if data is not None:
        if headers.get("Content-Type") == "application/x-www-form-urlencoded":
            data = urllib.parse.urlencode(data).encode("utf-8")
        else:
            headers["Content-Type"] = "application/json"
            data = json.dumps(data).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    
    try:
        with urllib.request.urlopen(req) as response:
            if response.status == 204:
                return {}
            response_data = response.read().decode("utf-8")
            return json.loads(response_data) if response_data else {}
    except urllib.error.HTTPError as e:
        error_data = e.read().decode("utf-8")
        if e.code == 409:
            # Conflict is sometimes expected (e.g. creating something that exists)
            return {"status": 409, "error": error_data}
        print(f"HTTPError: {e.code} {e.reason}", file=sys.stderr)
        print(error_data, file=sys.stderr)
        sys.exit(1)

def get_token(client_id: str, client_secret: str) -> str:
    response = request(
        POLARIS_TOKEN_URL,
        method="POST",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "realm": "POLARIS"
        },
    )
    return response["access_token"]


def provision() -> None:
    # Initial authentication with root credentials
    token = get_token("root", "s3cr3t")
    headers = {
        "Authorization": f"Bearer {token}",
        "realm": "POLARIS"
    }

    # 1. Create Principal
    principal_name = "iceberg_principal"
    principal_resp = request(
        f"{POLARIS_URL}/principals",
        method="POST",
        headers=headers,
        data={"name": principal_name, "type": "PRINCIPAL"},
    )
    
    if principal_resp.get("status") == 409:
        principal_resp = request(
            f"{POLARIS_URL}/principals/{principal_name}/rotate-credentials",
            method="POST",
            headers=headers,
        )
    
    credentials = principal_resp.get("credentials", {})
    client_id = credentials.get("clientId")
    client_secret = credentials.get("clientSecret")
    
    if not client_id or not client_secret:
        print("Failed to obtain credentials for principal", file=sys.stderr)
        sys.exit(1)

    # 2. Assign service_admin role to our principal
    request(
        f"{POLARIS_URL}/principals/{principal_name}/principal-roles",
        method="PUT",
        headers=headers,
        data={"principalRole": {"name": "service_admin"}},
    )

    # 3. Create Principal Role for catalog access
    role_name = "iceberg_role"
    request(
        f"{POLARIS_URL}/principal-roles",
        method="POST",
        headers=headers,
        data={"principalRole": {"name": role_name}},
    )

    # 4. Link Principal to Principal Role
    request(
        f"{POLARIS_URL}/principals/{principal_name}/principal-roles",
        method="PUT",
        headers=headers,
        data={"principalRole": {"name": role_name}},
    )

    # 5. Create Catalog
    catalog_name = "polaris"
    request(
        f"{POLARIS_URL}/catalogs",
        method="POST",
        headers=headers,
        data={
            "catalog": {
                "name": catalog_name,
                "type": "INTERNAL",
                "readOnly": False,
                "properties": {
                    "default-base-location": "s3://warehouse/polaris/",
                    "polaris.config.drop-with-purge.enabled": "true",
                },
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": ["s3://warehouse/polaris/"],
                    "region": "us-east-1",
                    "endpoint": "http://minio:9000",
                },
            }
        },
    )

    # 6. Link catalog_admin role to our principal role
    request(
        f"{POLARIS_URL}/principal-roles/{role_name}/catalog-roles/{catalog_name}",
        method="PUT",
        headers=headers,
        data={"catalogRole": {"name": "catalog_admin"}},
    )

    # 7. Grant explicit privileges to catalog_admin role for this catalog
    privileges = [
        "CATALOG_MANAGE_CONTENT",
        "CATALOG_MANAGE_METADATA",
        "TABLE_CREATE",
        "TABLE_WRITE_DATA",
        "TABLE_LIST",
        "NAMESPACE_CREATE",
        "NAMESPACE_LIST",
    ]
    
    for privilege in privileges:
        request(
            f"{POLARIS_URL}/catalogs/{catalog_name}/catalog-roles/catalog_admin/grants",
            method="PUT",
            headers=headers,
            data={"grant": {"type": "catalog", "privilege": privilege}},
        )

    # Print credentials for use in CI
    print(f"CLIENT_ID={client_id}")
    print(f"CLIENT_SECRET={client_secret}")


if __name__ == "__main__":
    provision()
