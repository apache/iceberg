---
title: Apache Iceberg REST Catalog API v1.0.0
language_tabs:
  - shell: Shell
  - java: Java
language_clients:
  - shell: ""
  - java: ""
toc_footers: []
includes: []
search: true
highlight_theme: darkula
headingLevel: 2

---

<!-- Generator: Widdershins v4.0.1 -->

<h1 id="apache-iceberg-rest-catalog-api">Apache Iceberg REST Catalog API v1.0.0</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

Defines the specification for the first version of the REST Catalog API. Implementations should support both Iceberg table specs v1 and v2, with priority given to v2.

Base URLs:

* <a href="http://127.0.0.1:1080">http://127.0.0.1:1080</a>

License: <a href="https://www.apache.org/licenses/LICENSE-2.0.html">Apache 2.0</a>

# Authentication

- HTTP Authentication, scheme: bearer 

<h1 id="apache-iceberg-rest-catalog-api-configuration-api">Configuration API</h1>

## getConfig

<a id="opIdgetConfig"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:1080/v1/config \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/config");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`GET /v1/config`

*List all catalog configuration settings*

All REST catalogs will be initialized by calling this route. This route will return at least the minimum necessary metadata to initialize the catalog. Optionally, it can also include server-side specific overrides. For example, it might also include information used to initialize this catalog such as the details of the Http connection pooling, etc. This route might also advertise information about operations that are not implemented so that the catalog can eagerly throw or go about another way of performing the desired action.

> Example responses

> default Response

```json
{
  "rootPath": "/v1",
  "catalogProperties": {}
}
```

<h3 id="getconfig-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Unknown Error|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|default|Default|Server-Specific Configuration Overrides|[IcebergConfiguration](#schemaicebergconfiguration)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## postConfig

<a id="opIdpostConfig"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:1080/v1/catalogs/{catalog} \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/catalogs/{catalog}");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`POST /v1/catalogs/{catalog}`

*Persist catalog specific configuration, which can be retrieved for later use.*

Persist some catalog specific configurations, which will be returned by \ calls to /v1/config in the future. This is basically all of the data \ that would go into the Catalog's `initialize` call.

> Body parameter

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "name": "string",
  "location": "string",
  "properties": {}
}
```

<h3 id="postconfig-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[Catalog](#schemacatalog)|true|none|
|» id|body|string(uuid)|false|Unique identifier for this catalog|
|» name|body|string|true|none|
|» location|body|string|false|Warehouse location for this catalog or URI of metastore or other identifying location|
|» properties|body|object|true|Additional catalog level properties|
|catalog|path|string|true|Name of the catalog being configured|

<h3 id="postconfig-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

<h1 id="apache-iceberg-rest-catalog-api-catalog-api">Catalog API</h1>

## loadTable

<a id="opIdloadTable"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:1080/v1/namespaces/{namespace}/tables/{table} \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}/tables/{table}");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`GET /v1/namespaces/{namespace}/tables/{table}`

*Load a given table from a given namespace*

<h3 id="loadtable-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|Namespace the table is in|
|table|path|string|true|Name of the table to load|

> Example responses

> 200 Response

```json
{
  "identifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  },
  "location": "string",
  "metadataLocation": "string",
  "metadataJson": "string",
  "schema": {
    "aliases": {
      "property1": 0,
      "property2": 0
    }
  },
  "partitionSpec": {
    "unpartitioned": true
  },
  "properties": {
    "property1": "string",
    "property2": "string"
  }
}
```

<h3 id="loadtable-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[GetTableResponse](#schemagettableresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|412|[Precondition Failed](https://tools.ietf.org/html/rfc7232#section-4.2)|NoSuchTableException|[NoSuchTableError](#schemanosuchtableerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## commitTable

<a id="opIdcommitTable"></a>

> Code samples

```shell
# You can also use wget
curl -X PUT http://127.0.0.1:1080/v1/namespaces/{namespace}/tables/{table} \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}/tables/{table}");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("PUT");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`PUT /v1/namespaces/{namespace}/tables/{table}`

*Commit an in progress create (or replace) table transaction*

Commit a pending create (or replace) table transaction, e.g. for doCommit.

> Body parameter

```json
{
  "tableIdentifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  },
  "metadataJson": "string"
}
```

<h3 id="committable-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CommitTableRequest](#schemacommittablerequest)|true|none|
|» tableIdentifier|body|[TableIdentifier](#schematableidentifier)|false|none|
|»» namespace|body|[string]|true|none|
|»» name|body|string|false|none|
|» metadataJson|body|string|false|none|
|namespace|path|string|true|Namespace the table is in|
|table|path|string|true|Name of the table to load|

> Example responses

> 200 Response

```json
{
  "metadataLocation": "string",
  "metadataJson": "string"
}
```

<h3 id="committable-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[CommitTableResponse](#schemacommittableresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|[IcebergResponseObject](#schemaicebergresponseobject)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## dropTable

<a id="opIddropTable"></a>

> Code samples

```shell
# You can also use wget
curl -X DELETE http://127.0.0.1:1080/v1/namespaces/{namespace}/tables/{table} \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}/tables/{table}");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("DELETE");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`DELETE /v1/namespaces/{namespace}/tables/{table}`

*Drop a table from the catalog, optionally purging the underlying data*

Remove a table from the catalog, optionally dropping the underlying data

<h3 id="droptable-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|purge|query|boolean|false|none|
|namespace|path|string|true|Namespace the table is in|
|table|path|string|true|Name of the table to load|

> Example responses

> 200 Response

```json
true
```

<h3 id="droptable-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|boolean|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## tableExists

<a id="opIdtableExists"></a>

> Code samples

```shell
# You can also use wget
curl -X HEAD http://127.0.0.1:1080/v1/namespaces/{namespace}/tables/{table} \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}/tables/{table}");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("HEAD");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`HEAD /v1/namespaces/{namespace}/tables/{table}`

*Check if a table exists*

Check if a table exists within a given namespace. Returns the standard response with `true` when found. Will return a TableNotFound error if not present.

<h3 id="tableexists-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|
|table|path|string|true|none|

<h3 id="tableexists-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|None|
|412|[Precondition Failed](https://tools.ietf.org/html/rfc7232#section-4.2)|Table Not Found|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## listTables

<a id="opIdlistTables"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:1080/v1/namespaces/{namespace}/tables \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}/tables");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`GET /v1/namespaces/{namespace}/tables`

*List all table identifiers underneath a given namespace*

Return all table identifiers under this namespace

<h3 id="listtables-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|Namespace under which to list tables.|
|limit|query|integer|false|number of values to return in one request|
|offset|query|integer|false|Place in the response to continue from if paginating|

> Example responses

> 200 Response

```json
{
  "identifiers": [
    {
      "namespace": [
        "string"
      ],
      "name": "string"
    }
  ]
}
```

<h3 id="listtables-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[ListTablesResponse](#schemalisttablesresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## createTable

<a id="opIdcreateTable"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:1080/v1/namespaces/{namespace}/tables \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}/tables");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`POST /v1/namespaces/{namespace}/tables`

*Create a table with the identifier given in the body*

> Body parameter

```json
{
  "identifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  },
  "schema": {
    "aliases": {
      "property1": 0,
      "property2": 0
    }
  },
  "partitionSpec": {
    "unpartitioned": true
  },
  "sortOrder": {
    "unsorted": true
  },
  "properties": {
    "property1": "string",
    "property2": "string"
  },
  "metadataJson": "string",
  "commit": true
}
```

<h3 id="createtable-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateTableRequest](#schemacreatetablerequest)|true|none|
|» identifier|body|[TableIdentifier](#schematableidentifier)|false|none|
|»» namespace|body|[string]|true|none|
|»» name|body|string|false|none|
|» schema|body|[Schema](#schemaschema)|false|none|
|»» aliases|body|object|false|none|
|»»» **additionalProperties**|body|integer(int32)|false|none|
|» partitionSpec|body|[PartitionSpec](#schemapartitionspec)|false|none|
|»» unpartitioned|body|boolean|false|none|
|» sortOrder|body|[SortOrder](#schemasortorder)|false|none|
|»» unsorted|body|boolean|false|none|
|» properties|body|object|false|none|
|»» **additionalProperties**|body|string|false|none|
|» metadataJson|body|string|false|none|
|» commit|body|boolean|false|none|
|namespace|path|string|true|Namespace under which to list tables.|

> Example responses

> 200 Response

```json
{
  "identifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  },
  "tableLocation": "s3://bucket/prod/accounting.db/monthly_sales",
  "metadataLocation": "s3://bucket/prod/accounting.db/monthly_sales/metadata/00001-0d4ef14f-ef7d-43e7-9af-5f4ad40a1103.metadata.json",
  "metadataJson": "TODO",
  "accessToken": "string"
}
```

<h3 id="createtable-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[CreateTableResponse](#schemacreatetableresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## renameTable

<a id="opIdrenameTable"></a>

> Code samples

```shell
# You can also use wget
curl -X PUT http://127.0.0.1:1080/v1/namespaces/{namespace}/tables \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}/tables");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("PUT");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`PUT /v1/namespaces/{namespace}/tables`

*Rename a table from its current name to a new name within the same catalog*

Rename a table within the same catalog

> Body parameter

```json
{
  "sourceTableIdentifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  },
  "destinationTableIdentifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  }
}
```

<h3 id="renametable-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[RenameTableRequest](#schemarenametablerequest)|true|Current table identifier to rename and new table identifier to rename to|
|» sourceTableIdentifier|body|[TableIdentifier](#schematableidentifier)|false|none|
|»» namespace|body|[string]|true|none|
|»» name|body|string|false|none|
|» destinationTableIdentifier|body|[TableIdentifier](#schematableidentifier)|false|none|
|namespace|path|string|true|Namespace under which to list tables.|

> Example responses

> 409 Response

```json
"{ error: { message: \"Namespace already exists\", type: \"AlreadyExistsException\", code: 40902 }"
```

> 412 Response

```json
"{ error: { message: \"Table does not exist\", type: \"NoSuchTableException\", code: 41202 }"
```

<h3 id="renametable-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|The new table identifier, the to table rename to, already exists.|[TableAlreadyExistsError](#schematablealreadyexistserror)|
|412|[Precondition Failed](https://tools.ietf.org/html/rfc7232#section-4.2)|Table to rename from does not exist|[NoSuchTableError](#schemanosuchtableerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## loadNamespaceMetadata

<a id="opIdloadNamespaceMetadata"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:1080/v1/namespaces/{namespace}/properties \
  -H 'Accept: application' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}/properties");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`GET /v1/namespaces/{namespace}/properties`

*Load the metadata properties for a namespace*

Return all stored metadata properties for a given namespace

<h3 id="loadnamespacemetadata-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|

> Example responses

> 200 Response

> 417 Response

```json
"{ error: { message: \"Namespace does not exist\", type: \"NoSuchNamespaceException\", code: 41701 }"
```

<h3 id="loadnamespacemetadata-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[GetNamespaceResponse](#schemagetnamespaceresponse)|
|417|[Expectation Failed](https://tools.ietf.org/html/rfc7231#section-6.5.14)|Namespace not found|[NoSuchNamespaceError](#schemanosuchnamespaceerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## setNamespaceProperties

<a id="opIdsetNamespaceProperties"></a>

> Code samples

```shell
# You can also use wget
curl -X PUT http://127.0.0.1:1080/v1/namespaces/{namespace}/properties \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}/properties");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("PUT");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`PUT /v1/namespaces/{namespace}/properties`

*Add or overwrite properties to an existing namespace*

Adds propertiess for a namespace. This will overwrite any existing properties, and merge with the others.

> Body parameter

```json
{
  "namespace": "string",
  "properties": [
    "string"
  ]
}
```

<h3 id="setnamespaceproperties-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[RemovePropertiesRequest](#schemaremovepropertiesrequest)|true|none|
|» namespace|body|string|false|none|
|» properties|body|[string]|false|none|
|namespace|path|string|true|none|

> Example responses

> 200 Response

```json
true
```

> 409 Response

> 417 Response

```json
"{ error: { message: \"Namespace does not exist\", type: \"NoSuchNamespaceException\", code: 41701 }"
```

<h3 id="setnamespaceproperties-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|boolean|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Namespace already exists|[NamespaceAlreadyExistsError](#schemanamespacealreadyexistserror)|
|417|[Expectation Failed](https://tools.ietf.org/html/rfc7231#section-6.5.14)|Namespace not found|[NoSuchNamespaceError](#schemanosuchnamespaceerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## setProperties

<a id="opIdsetProperties"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:1080/v1/namespaces/{namespace}/properties \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}/properties");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`POST /v1/namespaces/{namespace}/properties`

*Overwrite a namespace's properties with a new set of properties*

Set properties on a namespace

> Body parameter

```json
{
  "namespace": "string",
  "properties": {}
}
```

<h3 id="setproperties-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[SetPropertiesRequest](#schemasetpropertiesrequest)|true|none|
|» namespace|body|string|false|none|
|» properties|body|object|false|none|
|namespace|path|string|true|none|

> Example responses

> 200 Response

```json
"{ data: { success: true }, error: { } }"
```

<h3 id="setproperties-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|Inline|

<h3 id="setproperties-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## listNamespaces

<a id="opIdlistNamespaces"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:1080/v1/namespaces \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`GET /v1/namespaces`

*List all namespaces, or all namespaces underneath a given namespace*

List namespaces underneath a given namespace

<h3 id="listnamespaces-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|query|string|false|Namespace under which to list namespaces. Leave empty to list all namespaces in the catalog|

> Example responses

> 200 Response

```json
{
  "databases": [
    {
      "empty": true
    }
  ]
}
```

<h3 id="listnamespaces-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[ListNamespacesResponse](#schemalistnamespacesresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## createNamespace

<a id="opIdcreateNamespace"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:1080/v1/namespaces/{namespace} \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`POST /v1/namespaces/{namespace}`

*Create a namespace*

Create a namespace, with an optional set of properties. The server might also add properties.

> Body parameter

```json
{
  "namespace": [
    "string"
  ],
  "properties": "{ owner: \"Hank Bendickson\" }"
}
```

<h3 id="createnamespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateNamespaceRequest](#schemacreatenamespacerequest)|true|none|
|» namespace|body|[string]|false|individual levels of the namespace|
|» properties|body|object|false|Configuration properties for the namespace|
|namespace|path|string|true|none|

<h3 id="createnamespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## dropNamespace

<a id="opIddropNamespace"></a>

> Code samples

```shell
# You can also use wget
curl -X DELETE http://127.0.0.1:1080/v1/namespaces/{namespace} \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/v1/namespaces/{namespace}");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("DELETE");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

`DELETE /v1/namespaces/{namespace}`

*Drop a namespace from the catalog. Namespace must be empty.*

<h3 id="dropnamespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|

> Example responses

> 200 Response

```json
true
```

<h3 id="dropnamespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|boolean|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

# Schemas

<h2 id="tocS_CommitTableRequest">CommitTableRequest</h2>
<!-- backwards compatibility -->
<a id="schemacommittablerequest"></a>
<a id="schema_CommitTableRequest"></a>
<a id="tocScommittablerequest"></a>
<a id="tocscommittablerequest"></a>

```json
{
  "tableIdentifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  },
  "metadataJson": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|tableIdentifier|[TableIdentifier](#schematableidentifier)|false|none|none|
|metadataJson|string|false|none|none|

<h2 id="tocS_TableIdentifier">TableIdentifier</h2>
<!-- backwards compatibility -->
<a id="schematableidentifier"></a>
<a id="schema_TableIdentifier"></a>
<a id="tocStableidentifier"></a>
<a id="tocstableidentifier"></a>

```json
{
  "namespace": [
    "string"
  ],
  "name": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|[string]|true|none|none|
|name|string|false|none|none|

<h2 id="tocS_CommitTableResponse">CommitTableResponse</h2>
<!-- backwards compatibility -->
<a id="schemacommittableresponse"></a>
<a id="schema_CommitTableResponse"></a>
<a id="tocScommittableresponse"></a>
<a id="tocscommittableresponse"></a>

```json
{
  "metadataLocation": "string",
  "metadataJson": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|metadataLocation|string|false|none|none|
|metadataJson|string|false|none|none|

<h2 id="tocS_RemovePropertiesRequest">RemovePropertiesRequest</h2>
<!-- backwards compatibility -->
<a id="schemaremovepropertiesrequest"></a>
<a id="schema_RemovePropertiesRequest"></a>
<a id="tocSremovepropertiesrequest"></a>
<a id="tocsremovepropertiesrequest"></a>

```json
{
  "namespace": "string",
  "properties": [
    "string"
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|false|none|none|
|properties|[string]|false|none|none|

<h2 id="tocS_Catalog">Catalog</h2>
<!-- backwards compatibility -->
<a id="schemacatalog"></a>
<a id="schema_Catalog"></a>
<a id="tocScatalog"></a>
<a id="tocscatalog"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "name": "string",
  "location": "string",
  "properties": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string(uuid)|false|none|Unique identifier for this catalog|
|name|string|true|none|none|
|location|string|false|none|Warehouse location for this catalog or URI of metastore or other identifying location|
|properties|object|true|none|Additional catalog level properties|

<h2 id="tocS_CreateNamespaceRequest">CreateNamespaceRequest</h2>
<!-- backwards compatibility -->
<a id="schemacreatenamespacerequest"></a>
<a id="schema_CreateNamespaceRequest"></a>
<a id="tocScreatenamespacerequest"></a>
<a id="tocscreatenamespacerequest"></a>

```json
{
  "namespace": [
    "string"
  ],
  "properties": "{ owner: \"Hank Bendickson\" }"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|[string]|false|none|individual levels of the namespace|
|properties|object|false|none|Configuration properties for the namespace|

<h2 id="tocS_RenameTableRequest">RenameTableRequest</h2>
<!-- backwards compatibility -->
<a id="schemarenametablerequest"></a>
<a id="schema_RenameTableRequest"></a>
<a id="tocSrenametablerequest"></a>
<a id="tocsrenametablerequest"></a>

```json
{
  "sourceTableIdentifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  },
  "destinationTableIdentifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|sourceTableIdentifier|[TableIdentifier](#schematableidentifier)|false|none|none|
|destinationTableIdentifier|[TableIdentifier](#schematableidentifier)|false|none|none|

<h2 id="tocS_CreateTableRequest">CreateTableRequest</h2>
<!-- backwards compatibility -->
<a id="schemacreatetablerequest"></a>
<a id="schema_CreateTableRequest"></a>
<a id="tocScreatetablerequest"></a>
<a id="tocscreatetablerequest"></a>

```json
{
  "identifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  },
  "schema": {
    "aliases": {
      "property1": 0,
      "property2": 0
    }
  },
  "partitionSpec": {
    "unpartitioned": true
  },
  "sortOrder": {
    "unsorted": true
  },
  "properties": {
    "property1": "string",
    "property2": "string"
  },
  "metadataJson": "string",
  "commit": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|identifier|[TableIdentifier](#schematableidentifier)|false|none|none|
|schema|[Schema](#schemaschema)|false|none|none|
|partitionSpec|[PartitionSpec](#schemapartitionspec)|false|none|none|
|sortOrder|[SortOrder](#schemasortorder)|false|none|none|
|properties|object|false|none|none|
|» **additionalProperties**|string|false|none|none|
|metadataJson|string|false|none|none|
|commit|boolean|false|none|none|

<h2 id="tocS_PartitionSpec">PartitionSpec</h2>
<!-- backwards compatibility -->
<a id="schemapartitionspec"></a>
<a id="schema_PartitionSpec"></a>
<a id="tocSpartitionspec"></a>
<a id="tocspartitionspec"></a>

```json
{
  "unpartitioned": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|unpartitioned|boolean|false|none|none|

<h2 id="tocS_Schema">Schema</h2>
<!-- backwards compatibility -->
<a id="schemaschema"></a>
<a id="schema_Schema"></a>
<a id="tocSschema"></a>
<a id="tocsschema"></a>

```json
{
  "aliases": {
    "property1": 0,
    "property2": 0
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|aliases|object|false|none|none|
|» **additionalProperties**|integer(int32)|false|none|none|

<h2 id="tocS_SortOrder">SortOrder</h2>
<!-- backwards compatibility -->
<a id="schemasortorder"></a>
<a id="schema_SortOrder"></a>
<a id="tocSsortorder"></a>
<a id="tocssortorder"></a>

```json
{
  "unsorted": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|unsorted|boolean|false|none|none|

<h2 id="tocS_CreateTableResponse">CreateTableResponse</h2>
<!-- backwards compatibility -->
<a id="schemacreatetableresponse"></a>
<a id="schema_CreateTableResponse"></a>
<a id="tocScreatetableresponse"></a>
<a id="tocscreatetableresponse"></a>

```json
{
  "identifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  },
  "tableLocation": "s3://bucket/prod/accounting.db/monthly_sales",
  "metadataLocation": "s3://bucket/prod/accounting.db/monthly_sales/metadata/00001-0d4ef14f-ef7d-43e7-9af-5f4ad40a1103.metadata.json",
  "metadataJson": "TODO",
  "accessToken": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|identifier|[TableIdentifier](#schematableidentifier)|false|none|none|
|tableLocation|string|false|none|Path of of the table just before the standard metadata / data folders. This is useful in cases where a different location provider might be used|
|metadataLocation|string|false|none|Location of the most current primary metadata file for the table|
|metadataJson|string|false|none|Stringified JSON representing the tables metadata|
|accessToken|string|false|none|none|

<h2 id="tocS_SetPropertiesRequest">SetPropertiesRequest</h2>
<!-- backwards compatibility -->
<a id="schemasetpropertiesrequest"></a>
<a id="schema_SetPropertiesRequest"></a>
<a id="tocSsetpropertiesrequest"></a>
<a id="tocssetpropertiesrequest"></a>

```json
{
  "namespace": "string",
  "properties": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|false|none|none|
|properties|object|false|none|none|

<h2 id="tocS_ListNamespacesResponse">ListNamespacesResponse</h2>
<!-- backwards compatibility -->
<a id="schemalistnamespacesresponse"></a>
<a id="schema_ListNamespacesResponse"></a>
<a id="tocSlistnamespacesresponse"></a>
<a id="tocslistnamespacesresponse"></a>

```json
{
  "databases": [
    {
      "empty": true
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|databases|[[Namespace](#schemanamespace)]|false|none|[Reference to one or more levels of a namespace]|

<h2 id="tocS_Namespace">Namespace</h2>
<!-- backwards compatibility -->
<a id="schemanamespace"></a>
<a id="schema_Namespace"></a>
<a id="tocSnamespace"></a>
<a id="tocsnamespace"></a>

```json
{
  "empty": true
}

```

Reference to one or more levels of a namespace

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|empty|boolean|false|none|none|

<h2 id="tocS_GetNamespaceResponse">GetNamespaceResponse</h2>
<!-- backwards compatibility -->
<a id="schemagetnamespaceresponse"></a>
<a id="schema_GetNamespaceResponse"></a>
<a id="tocSgetnamespaceresponse"></a>
<a id="tocsgetnamespaceresponse"></a>

```json
{
  "namespace": {
    "empty": true
  },
  "properties": {
    "property1": "string",
    "property2": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|[Namespace](#schemanamespace)|false|none|Reference to one or more levels of a namespace|
|properties|object|false|none|none|
|» **additionalProperties**|string|false|none|none|

<h2 id="tocS_ListTablesResponse">ListTablesResponse</h2>
<!-- backwards compatibility -->
<a id="schemalisttablesresponse"></a>
<a id="schema_ListTablesResponse"></a>
<a id="tocSlisttablesresponse"></a>
<a id="tocslisttablesresponse"></a>

```json
{
  "identifiers": [
    {
      "namespace": [
        "string"
      ],
      "name": "string"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|identifiers|[[TableIdentifier](#schematableidentifier)]|false|none|none|

<h2 id="tocS_GetTableResponse">GetTableResponse</h2>
<!-- backwards compatibility -->
<a id="schemagettableresponse"></a>
<a id="schema_GetTableResponse"></a>
<a id="tocSgettableresponse"></a>
<a id="tocsgettableresponse"></a>

```json
{
  "identifier": {
    "namespace": [
      "string"
    ],
    "name": "string"
  },
  "location": "string",
  "metadataLocation": "string",
  "metadataJson": "string",
  "schema": {
    "aliases": {
      "property1": 0,
      "property2": 0
    }
  },
  "partitionSpec": {
    "unpartitioned": true
  },
  "properties": {
    "property1": "string",
    "property2": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|identifier|[TableIdentifier](#schematableidentifier)|false|none|none|
|location|string|false|none|none|
|metadataLocation|string|false|none|none|
|metadataJson|string|false|none|none|
|schema|[Schema](#schemaschema)|false|none|none|
|partitionSpec|[PartitionSpec](#schemapartitionspec)|false|none|none|
|properties|object|false|none|none|
|» **additionalProperties**|string|false|none|none|

<h2 id="tocS_TableMetadata">TableMetadata</h2>
<!-- backwards compatibility -->
<a id="schematablemetadata"></a>
<a id="schema_TableMetadata"></a>
<a id="tocStablemetadata"></a>
<a id="tocstablemetadata"></a>

```json
{
  "currentSnapshotTo": {
    "currentSnapshotTo": {}
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|currentSnapshotTo|[TableMetadata](#schematablemetadata)|false|none|none|

<h2 id="tocS_IcebergConfiguration">IcebergConfiguration</h2>
<!-- backwards compatibility -->
<a id="schemaicebergconfiguration"></a>
<a id="schema_IcebergConfiguration"></a>
<a id="tocSicebergconfiguration"></a>
<a id="tocsicebergconfiguration"></a>

```json
{
  "rootPath": "/v1",
  "catalogProperties": {}
}

```

Server-provided configuration for the catalog.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|rootPath|string|false|none|Root path to be used for all other requests. Server-side implementations are free to use another choice for root path, but must conform to the specification otherwise in order to interoperate with the URI generation that the REST catalog client will do.|
|catalogProperties|object¦null|false|none|An optional field for storing catalog configuration properties that are stored server side. This could be beneifical to an administrator, for example to enforce that a given LocationProvider is used or to enforce that a certain FileIO implementation is used.|

<h2 id="tocS_NoSuchNamespaceError">NoSuchNamespaceError</h2>
<!-- backwards compatibility -->
<a id="schemanosuchnamespaceerror"></a>
<a id="schema_NoSuchNamespaceError"></a>
<a id="tocSnosuchnamespaceerror"></a>
<a id="tocsnosuchnamespaceerror"></a>

```json
{
  "message": "string",
  "type": "string",
  "code": 40101,
  "metadata": {}
}

```

Namespace provided in the request does not exist

### Properties

*None*

<h2 id="tocS_NamespaceAlreadyExistsError">NamespaceAlreadyExistsError</h2>
<!-- backwards compatibility -->
<a id="schemanamespacealreadyexistserror"></a>
<a id="schema_NamespaceAlreadyExistsError"></a>
<a id="tocSnamespacealreadyexistserror"></a>
<a id="tocsnamespacealreadyexistserror"></a>

```json
{
  "message": "string",
  "type": "string",
  "code": 40101,
  "metadata": {}
}

```

Namespace provided in the request already exists

### Properties

*None*

<h2 id="tocS_NoSuchTableError">NoSuchTableError</h2>
<!-- backwards compatibility -->
<a id="schemanosuchtableerror"></a>
<a id="schema_NoSuchTableError"></a>
<a id="tocSnosuchtableerror"></a>
<a id="tocsnosuchtableerror"></a>

```json
{
  "message": "string",
  "type": "string",
  "code": 40101,
  "metadata": {}
}

```

The given table identifier does not exist

### Properties

*None*

<h2 id="tocS_TableAlreadyExistsError">TableAlreadyExistsError</h2>
<!-- backwards compatibility -->
<a id="schematablealreadyexistserror"></a>
<a id="schema_TableAlreadyExistsError"></a>
<a id="tocStablealreadyexistserror"></a>
<a id="tocstablealreadyexistserror"></a>

```json
{
  "message": "string",
  "type": "string",
  "code": 40101,
  "metadata": {}
}

```

the given table identifier already exists and cannot be created / renamed to

### Properties

*None*

<h2 id="tocS_IcebergResponseObject">IcebergResponseObject</h2>
<!-- backwards compatibility -->
<a id="schemaicebergresponseobject"></a>
<a id="schema_IcebergResponseObject"></a>
<a id="tocSicebergresponseobject"></a>
<a id="tocsicebergresponseobject"></a>

```json
"{ data: { }, error: { } }"

```

JSON wrapper for all response bodies, with a data object and / or an error object

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[ResponseDataObject](#schemaresponsedataobject)|false|none|JSON data payload returned in a successful response body.|
|error|[ResponseErrorObject](#schemaresponseerrorobject)|false|none|Error portion embedded in all JSON responses with further details of the error|

<h2 id="tocS_ResponseDataObject">ResponseDataObject</h2>
<!-- backwards compatibility -->
<a id="schemaresponsedataobject"></a>
<a id="schema_ResponseDataObject"></a>
<a id="tocSresponsedataobject"></a>
<a id="tocsresponsedataobject"></a>

```json
"{ data: { identifiers: [ \"office.employees\", \"office.dogs\", \"office.cats\" ] }"

```

JSON data payload returned in a successful response body.

### Properties

*None*

<h2 id="tocS_ResponseErrorObject">ResponseErrorObject</h2>
<!-- backwards compatibility -->
<a id="schemaresponseerrorobject"></a>
<a id="schema_ResponseErrorObject"></a>
<a id="tocSresponseerrorobject"></a>
<a id="tocsresponseerrorobject"></a>

```json
{
  "message": "string",
  "type": "string",
  "code": 40101,
  "metadata": {}
}

```

Error portion embedded in all JSON responses with further details of the error

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|message|string|true|none|Human-readable error message|
|type|string|true|none|Machine-type of the errror, such as an exception class|
|code|integer|true|none|Application specific error code, to disambiguage amongst subclasses of HTTP responses|
|metadata|object¦null|false|none|Additional metadata to accompany this error, such as server side stack traces or user instructions|

