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
curl -X GET http://127.0.0.1:1080/config \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/config");
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

`GET /config`

*List all catalog configuration settings*

All REST catalog clients will first call this route to get some configuration provided by the server. This route will return any server specified default configuration values for the catalog, such as configuration values used to setup the catalog for usage with Spark (e.g. vectorization-enabled).
Users should be able to override these values with client specified values.
The server might be able to request that the client use its value over a value that has been configured in the client application. How and if it will do that is an open question, and thus not currently specified in this documents schema.

> Example responses

> default Response

```json
{
  "rootPath": "/",
  "catalogProperties": {}
}
```

<h3 id="getconfig-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Unknown Error|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|default|Default|Server-Specific Configuration Values (or Overrides)|[IcebergConfiguration](#schemaicebergconfiguration)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

<h1 id="apache-iceberg-rest-catalog-api-catalog-api">Catalog API</h1>

## listNamespaces

<a id="opIdlistNamespaces"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:1080/namespaces \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/namespaces");
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

`GET /namespaces`

*List namespaces, optionally providing a parent namespace to list underneaath*

List all namespaces at a certain level, optionally starting from a given parent namespace. For example, if table a.b.t exists, using 'SELECT NAMESPACE IN a' this would translate into `GET /namespaces?parent=a` and must return Namepace.of("a","b").

<h3 id="listnamespaces-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|parent|query|string|false|Optional parent namespace under which to list namespaces. When empty, list top-level namespaces.|

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
curl -X POST http://127.0.0.1:1080/namespaces \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/namespaces");
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

`POST /namespaces`

*Create a namespace*

Create a namespace, with an optional set of properties. The server might also add properties, such as last_modified_time etc.

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

> Example responses

> 409 Response

```json
"{ error: { message: \"Namespace already exists\", type: \"AlreadyExistsException\", code: 40901 }"
```

<h3 id="createnamespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Conflict (AlreadyExistsException)|[NamespaceAlreadyExistsError](#schemanamespacealreadyexistserror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## loadNamespaceMetadata

<a id="opIdloadNamespaceMetadata"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:1080/namespaces/{namespace} \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/namespaces/{namespace}");
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

`GET /namespaces/{namespace}`

*Load the metadata properties for a namespace*

Return all stored metadata properties for a given namespace

<h3 id="loadnamespacemetadata-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|

> Example responses

> 200 Response

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

> 404 Response

```json
"{ error: { message: \"Namespace does not exist\", type: \"NoSuchNamespaceException\", code: 40401 }"
```

<h3 id="loadnamespacemetadata-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[GetNamespaceResponse](#schemagetnamespaceresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (NoSuchNamespaceException)|[NoSuchNamespaceError](#schemanosuchnamespaceerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## dropNamespace

<a id="opIddropNamespace"></a>

> Code samples

```shell
# You can also use wget
curl -X DELETE http://127.0.0.1:1080/namespaces/{namespace} \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/namespaces/{namespace}");
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

`DELETE /namespaces/{namespace}`

*Drop a namespace from the catalog. Namespace must be empty.*

<h3 id="dropnamespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "dropped": true
  }
}
```

<h3 id="dropnamespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[IcebergResponseObject](#schemaicebergresponseobject)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## setProperties

<a id="opIdsetProperties"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:1080/namespaces/{namespace}/properties \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/namespaces/{namespace}/properties");
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

`POST /namespaces/{namespace}/properties`

*Set a collection of properties on a namespace*

Set a collection of properties on a namespace in the catalog. Properties that are not in the given map are not modified or removed by this call. Server implementations are not required to support namespace properties.

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
{
  "updated": true
}
```

> 404 Response

```json
"{ error: { message: \"Namespace does not exist\", type: \"NoSuchNamespaceException\", code: 40401 }"
```

<h3 id="setproperties-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[SetPropertiesResponse](#schemasetpropertiesresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (NoSuchNamespaceException)|[NoSuchNamespaceError](#schemanosuchnamespaceerror)|
|406|[Not Acceptable](https://tools.ietf.org/html/rfc7231#section-6.5.6)|Not Acceptable (Unsupported Operation)|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## removeProperties

<a id="opIdremoveProperties"></a>

> Code samples

```shell
# You can also use wget
curl -X PUT http://127.0.0.1:1080/namespaces/{namespace}/properties \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/namespaces/{namespace}/properties");
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

`PUT /namespaces/{namespace}/properties`

*Remove a set of property keys from a namespace in the catalog*

Remove a set of property keys from a namespace in the catalog. Properties that are not in the given set are not modified or removed by this call. Server implementations are not required to support namespace properties.

> Body parameter

```json
{
  "namespace": "string",
  "properties": {}
}
```

<h3 id="removeproperties-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[RemovePropertiesRequest](#schemaremovepropertiesrequest)|true|none|
|» namespace|body|string|false|none|
|» properties|body|object|false|none|
|namespace|path|string|true|none|

> Example responses

> 200 Response

```json
true
```

> 404 Response

```json
"{ error: { message: \"Namespace does not exist\", type: \"NoSuchNamespaceException\", code: 40401 }"
```

<h3 id="removeproperties-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|boolean|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (No Such Namespace)|[NoSuchNamespaceError](#schemanosuchnamespaceerror)|
|406|[Not Acceptable](https://tools.ietf.org/html/rfc7231#section-6.5.6)|Not Acceptable (Unsupported Operation)|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## loadTable

<a id="opIdloadTable"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:1080/namespaces/{namespace}/tables/{table} \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/namespaces/{namespace}/tables/{table}");
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

`GET /namespaces/{namespace}/tables/{table}`

*Load a given table from a given namespace*

<h3 id="loadtable-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|A namespace identifier|
|table|path|string|true|A table name|

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
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (NoSuchTableException | NoSuchNamespaceException)|Inline|

<h3 id="loadtable-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## dropTable

<a id="opIddropTable"></a>

> Code samples

```shell
# You can also use wget
curl -X DELETE http://127.0.0.1:1080/namespaces/{namespace}/tables/{table} \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/namespaces/{namespace}/tables/{table}");
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

`DELETE /namespaces/{namespace}/tables/{table}`

*Drop a table from the catalog*

Remove a table from the catalog

<h3 id="droptable-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|purgeRequested|query|boolean|false|Whether the user requested to purge the underlying table's data and metadata|
|namespace|path|string|true|A namespace identifier|
|table|path|string|true|A table name|

> Example responses

> 200 Response

```json
true
```

<h3 id="droptable-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|boolean|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|Accepted - for use if purgeRequested is implemented as an asynchronous action.|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## tableExists

<a id="opIdtableExists"></a>

> Code samples

```shell
# You can also use wget
curl -X HEAD http://127.0.0.1:1080/namespaces/{namespace}/tables/{table} \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/namespaces/{namespace}/tables/{table}");
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

`HEAD /namespaces/{namespace}/tables/{table}`

*Check if a table exists*

Check if a table exists within a given namespace. Returns the standard response with `true` when found. Will throw a NoSuchTableException if not present.

<h3 id="tableexists-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|A namespace identifier|
|table|path|string|true|none|

<h3 id="tableexists-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (NoSuchTableException)|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## listTables

<a id="opIdlistTables"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:1080/namespaces/{namespace}/tables \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/namespaces/{namespace}/tables");
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

`GET /namespaces/{namespace}/tables`

*List all table identifiers underneath a given namespace*

Return all table identifiers under this namespace

<h3 id="listtables-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|A namespace identifier under which to list tables|

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

## renameTable

<a id="opIdrenameTable"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:1080/tables/rename \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("http://127.0.0.1:1080/tables/rename");
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

`POST /tables/rename`

*Rename a table from its current name to a new name*

Rename a table from one identifier to another. It's valid to move a table across namespaces, but the server implementation doesn't need to support it.

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

> Example responses

> Not Found (NoSuchTableException - Table to rename does not exist | NoSuchTableException - The target namespace of the new table identifier does not exist))

```json
"{ error: { message: \"Table does not exist\", type: \"NoSuchTableException\", code: 40402 }"
```

```json
"{ error: { message: \"Namespace does not exist\", type: \"NoSuchNameSpaceException\", code: 40401 }"
```

> 409 Response

```json
"{ error: { message: \"Tale already exists\", type: \"AlreadyExistsException\", code: 40902 }"
```

<h3 id="renametable-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (NoSuchTableException - Table to rename does not exist | NoSuchTableException - The target namespace of the new table identifier does not exist))|Inline|
|406|[Not Acceptable](https://tools.ietf.org/html/rfc7231#section-6.5.6)|Not Acceptable (UnsupportedOperationException)|None|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Conflict (AlreadyExistsException - The new target table identifier already exists)|[TableAlreadyExistsError](#schematablealreadyexistserror)|

<h3 id="renametable-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

# Schemas

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

<h2 id="tocS_RemovePropertiesRequest">RemovePropertiesRequest</h2>
<!-- backwards compatibility -->
<a id="schemaremovepropertiesrequest"></a>
<a id="schema_RemovePropertiesRequest"></a>
<a id="tocSremovepropertiesrequest"></a>
<a id="tocsremovepropertiesrequest"></a>

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

<h2 id="tocS_SetPropertiesResponse">SetPropertiesResponse</h2>
<!-- backwards compatibility -->
<a id="schemasetpropertiesresponse"></a>
<a id="schema_SetPropertiesResponse"></a>
<a id="tocSsetpropertiesresponse"></a>
<a id="tocssetpropertiesresponse"></a>

```json
{
  "updated": true
}

```

JSON data response on a successful set properties call.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|updated|boolean|false|none|true if the set properties request was accepted and is reflected on the server|

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
  "rootPath": "/",
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
  "code": "40401, Table not found vs 40402, Namespace not found.",
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
  "code": "40401, Table not found vs 40402, Namespace not found.",
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
  "code": "40401, Table not found vs 40402, Namespace not found.",
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
  "code": "40401, Table not found vs 40402, Namespace not found.",
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
"{ \"data\": { \"namespaces\": [\"ns1.foo_db\", \"ns1.bar_db\"] }, \"error\": {} }"

```

JSON wrapper for all response bodies, with a data object and / or an error object

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[ResponseDataObject](#schemaresponsedataobject)|false|none|JSON data payload returned in a successful response body|
|error|[ResponseErrorObject](#schemaresponseerrorobject)|false|none|JSON error payload returned in a response with further details on the error|

<h2 id="tocS_ResponseDataObject">ResponseDataObject</h2>
<!-- backwards compatibility -->
<a id="schemaresponsedataobject"></a>
<a id="schema_ResponseDataObject"></a>
<a id="tocSresponsedataobject"></a>
<a id="tocsresponsedataobject"></a>

```json
"{ \"data\": { \"identifiers\": [ \"office.employees\", \"office.dogs\", \"office.cats\" ] }"

```

JSON data payload returned in a successful response body

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
  "code": "40401, Table not found vs 40402, Namespace not found.",
  "metadata": {}
}

```

JSON error payload returned in a response with further details on the error

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|message|string|true|none|Human-readable error message|
|type|string|true|none|Machine-type of the error, such as an exception class|
|code|integer|false|none|Application specific error code, to disambiguage amongst subclasses of HTTP responses|
|metadata|object¦null|false|none|Additional metadata to accompany this error, such as server side stack traces or user instructions|

