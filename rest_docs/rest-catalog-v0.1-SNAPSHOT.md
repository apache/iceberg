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

* <a href="https://{host}:{port}/{basePath}">https://{host}:{port}/{basePath}</a>

    * **host** - The host address for the specified server Default: localhost

    * **port** - The port used when addressing the host Default: 443

    * **basePath** -  Default: v1

* <a href="http://127.0.0.1:1080/v1">http://127.0.0.1:1080/v1</a>

License: <a href="https://www.apache.org/licenses/LICENSE-2.0.html">Apache 2.0</a>

# Authentication

- HTTP Authentication, scheme: bearer 

<h1 id="apache-iceberg-rest-catalog-api-configuration-api">Configuration API</h1>

## getConfig

<a id="opIdgetConfig"></a>

> Code samples

```shell
# You can also use wget
curl -X GET https://{host}:{port}/{basePath}/config \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/config");
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
curl -X GET https://{host}:{port}/{basePath}/namespaces \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/namespaces");
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
  "namespaces": [
    [
      "accounting",
      "tax"
    ]
  ]
}
```

<h3 id="listnamespaces-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[ListNamespacesResponse](#schemalistnamespacesresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (Parent namespace does not exist)|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## createNamespace

<a id="opIdcreateNamespace"></a>

> Code samples

```shell
# You can also use wget
curl -X POST https://{host}:{port}/{basePath}/namespaces \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/namespaces");
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
  "properties": "{ \"owner\": \"Hank Bendickson\" }"
}
```

<h3 id="createnamespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateNamespaceRequest](#schemacreatenamespacerequest)|true|none|
|» namespace|body|[string]|true|Individual levels of the namespace|
|» properties|body|object|false|Configuration properties for the namespace|

> Example responses

> 409 Response

```json
{
  "error": {
    "message": "Namespace already exists",
    "type": "AlreadyExistsException",
    "code": 409
  }
}
```

<h3 id="createnamespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|The specified namespace provided in the request already exists|[ResponseErrorObject](#schemaresponseerrorobject)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## loadNamespaceMetadata

<a id="opIdloadNamespaceMetadata"></a>

> Code samples

```shell
# You can also use wget
curl -X GET https://{host}:{port}/{basePath}/namespaces/{namespace} \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/namespaces/{namespace}");
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
  "namespace": [
    "accounting",
    "tax"
  ],
  "properties": {
    "owner": "Ralph",
    "transient_lastDdlTime": "1452120468"
  }
}
```

> 404 Response

```json
{
  "error": {
    "message": "Namespace does not exist",
    "type": "NoSuchNamespaceException",
    "code": 404
  }
}
```

<h3 id="loadnamespacemetadata-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[GetNamespaceResponse](#schemagetnamespaceresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (The specified namespace was not found)|[ResponseErrorObject](#schemaresponseerrorobject)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## namespaceExists

<a id="opIdnamespaceExists"></a>

> Code samples

```shell
# You can also use wget
curl -X HEAD https://{host}:{port}/{basePath}/namespaces/{namespace} \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/namespaces/{namespace}");
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

`HEAD /namespaces/{namespace}`

*Check if a namespace exists*

Check if a namespace exists.

<h3 id="namespaceexists-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|

<h3 id="namespaceexists-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK - Namesapce exists|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## dropNamespace

<a id="opIddropNamespace"></a>

> Code samples

```shell
# You can also use wget
curl -X DELETE https://{host}:{port}/{basePath}/namespaces/{namespace} \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/namespaces/{namespace}");
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
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[#/components/responses/IcebergResponseObject](#schema#/components/responses/icebergresponseobject)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## setProperties _ removeProperties

<a id="opIdsetProperties / removeProperties"></a>

> Code samples

```shell
# You can also use wget
curl -X POST https://{host}:{port}/{basePath}/namespaces/{namespace}/properties \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/namespaces/{namespace}/properties");
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

*Set or remove properties on a namespace*

Set and/or remove a collection or properties on a namespae. The request body specifies a list of properties to remove and a map of key value pairs to update.
Properties that are not in the request are not modified or removed by this call. Server implementations are not required to support namespace properties.

> Body parameter

```json
{
  "toRemove": "[ \"department\", \"access_group\" ]",
  "toUpdate": {
    "owner": "Hank Bendickson"
  }
}
```

<h3 id="setproperties-_-removeproperties-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[UpdatePropertiesRequest](#schemaupdatepropertiesrequest)|true|none|
|» toRemove|body|[string]|false|none|
|» toUpdate|body|object|false|none|
|namespace|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "updated": [
      "owner"
    ],
    "removed": [
      "foo"
    ],
    "notPresent": [
      "bar"
    ]
  }
}
```

> 404 Response

```json
{
  "error": {
    "message": "Namespace does not exist",
    "type": "NoSuchNamespaceException",
    "code": 404
  }
}
```

<h3 id="setproperties-_-removeproperties-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[UpdatePropertiesResponse](#schemaupdatepropertiesresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (NoSuchNamespaceException)|[#/components/responses/IcebergResponseObject](#schema#/components/responses/icebergresponseobject)|
|406|[Not Acceptable](https://tools.ietf.org/html/rfc7231#section-6.5.6)|Not Acceptable (Unsupported Operation)|None|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Unprocessable Entity. A property key was included in both toRemove and toUpdate.|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## listTables

<a id="opIdlistTables"></a>

> Code samples

```shell
# You can also use wget
curl -X GET https://{host}:{port}/{basePath}/namespaces/{namespace}/tables \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/namespaces/{namespace}/tables");
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

> 404 Response

```json
"{ error: { message: \"Namespace does not exist\", type: \"NoSuchNamespaceException\", code: 404 }"
```

<h3 id="listtables-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[ListTablesResponse](#schemalisttablesresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (NoSuchNamespaceException)|[#/components/responses/IcebergResponseObject](#schema#/components/responses/icebergresponseobject)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## dropTable

<a id="opIddropTable"></a>

> Code samples

```shell
# You can also use wget
curl -X DELETE https://{host}:{port}/{basePath}/namespaces/{namespace}/tables/{table} \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/namespaces/{namespace}/tables/{table}");
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
{
  "data": {
    "dropped": true,
    "purged": false
  }
}
```

> 404 Response

```json
{
  "error": {
    "message": "The given table does not exist",
    "type": "NoSuchTableException",
    "code": 404
  }
}
```

<h3 id="droptable-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[DropTableResponse](#schemadroptableresponse)|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|Accepted - for use if purgeRequested is implemented as an asynchronous action.|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found (The specified table identifier was not found)|[ResponseErrorObject](#schemaresponseerrorobject)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## tableExists

<a id="opIdtableExists"></a>

> Code samples

```shell
# You can also use wget
curl -X HEAD https://{host}:{port}/{basePath}/namespaces/{namespace}/tables/{table} \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/namespaces/{namespace}/tables/{table}");
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

Check if a table exists within a given namespace.

<h3 id="tableexists-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|A namespace identifier|
|table|path|string|true|A table name|

<h3 id="tableexists-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK - Table Exists|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## renameTable

<a id="opIdrenameTable"></a>

> Code samples

```shell
# You can also use wget
curl -X POST https://{host}:{port}/{basePath}/tables/rename \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer {access-token}'

```

```java
URL obj = new URL("https://{host}:{port}/{basePath}/tables/rename");
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
|»» namespace|body|[string]|true|Individual levels of the namespace|
|»» name|body|string|false|none|
|» destinationTableIdentifier|body|[TableIdentifier](#schematableidentifier)|false|none|

> Example responses

> Not Found - NoSuchTableException, Table to rename does not exist - NoSuchNamespaceException, The target namespace of the new table identifier does not exist

```json
{
  "error": {
    "message": "Table to rename does not exist",
    "type": "NoSuchTableException",
    "code": 404
  }
}
```

```json
{
  "error": {
    "message": "Namespace to rename to does not exist",
    "type": "NoSuchNameSpaceException",
    "code": 404
  }
}
```

> 409 Response

```json
{
  "error": {
    "message": "Table already exists",
    "type": "AlreadyExistsException",
    "code": 409
  }
}
```

<h3 id="renametable-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Unauthorized|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not Found - NoSuchTableException, Table to rename does not exist - NoSuchNamespaceException, The target namespace of the new table identifier does not exist|Inline|
|406|[Not Acceptable](https://tools.ietf.org/html/rfc7231#section-6.5.6)|Not Acceptable (UnsupportedOperationException)|None|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Conflict (AlreadyExistsException - The new target table identifier already exists)|[TableAlreadyExistsError](#schematablealreadyexistserror)|

<h3 id="renametable-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

# Schemas

<h2 id="tocS_ResponseDataObject">ResponseDataObject</h2>
<!-- backwards compatibility -->
<a id="schemaresponsedataobject"></a>
<a id="schema_ResponseDataObject"></a>
<a id="tocSresponsedataobject"></a>
<a id="tocsresponsedataobject"></a>

```json
{
  "data": {
    "identifiers": [
      "office.employees",
      "office.dogs",
      "office.cats"
    ]
  }
}

```

JSON data payload returned in a successful response body

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|object|false|none|Wrapper for the response of a successful request|

<h2 id="tocS_ResponseErrorObject">ResponseErrorObject</h2>
<!-- backwards compatibility -->
<a id="schemaresponseerrorobject"></a>
<a id="schema_ResponseErrorObject"></a>
<a id="tocSresponseerrorobject"></a>
<a id="tocsresponseerrorobject"></a>

```json
{
  "message": "string",
  "type": "NoSuchNamespaceException",
  "code": 404,
  "metadata": {}
}

```

JSON error payload returned in a response with further details on the error

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|message|string|true|none|Human-readable error message|
|type|string|true|none|Internal type of the error, such as an exception class|
|code|integer|true|none|HTTP response code|
|metadata|object¦null|false|none|Additional metadata to accompany this error, such as server side stack traces or user instructions|

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
|namespace|[string]|true|none|Individual levels of the namespace|
|name|string|false|none|none|

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
  "properties": "{ \"owner\": \"Hank Bendickson\" }"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|[string]|true|none|Individual levels of the namespace|
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

<h2 id="tocS_UpdatePropertiesRequest">UpdatePropertiesRequest</h2>
<!-- backwards compatibility -->
<a id="schemaupdatepropertiesrequest"></a>
<a id="schema_UpdatePropertiesRequest"></a>
<a id="tocSupdatepropertiesrequest"></a>
<a id="tocsupdatepropertiesrequest"></a>

```json
{
  "toRemove": "[ \"department\", \"access_group\" ]",
  "toUpdate": {
    "owner": "Hank Bendickson"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|toRemove|[string]|false|none|none|
|toUpdate|object|false|none|none|

<h2 id="tocS_UpdatePropertiesResponse">UpdatePropertiesResponse</h2>
<!-- backwards compatibility -->
<a id="schemaupdatepropertiesresponse"></a>
<a id="schema_UpdatePropertiesResponse"></a>
<a id="tocSupdatepropertiesresponse"></a>
<a id="tocsupdatepropertiesresponse"></a>

```json
{
  "updated": [
    "string"
  ],
  "removed": [
    "string"
  ],
  "notPresent": [
    "string"
  ]
}

```

JSON data response for a synchronous update properties request.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|updated|[string]|false|none|none|
|removed|[string]|false|none|List of properties that were removed (and not updated)|
|notPresent|[string]|false|none|none|

<h2 id="tocS_ListNamespacesResponse">ListNamespacesResponse</h2>
<!-- backwards compatibility -->
<a id="schemalistnamespacesresponse"></a>
<a id="schema_ListNamespacesResponse"></a>
<a id="tocSlistnamespacesresponse"></a>
<a id="tocslistnamespacesresponse"></a>

```json
{
  "namespaces": [
    [
      "accounting",
      "tax"
    ]
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespaces|[[Namespace](#schemanamespace)]|false|none|[Reference to one or more levels of a namespace]|

<h2 id="tocS_DropTableResponse">DropTableResponse</h2>
<!-- backwards compatibility -->
<a id="schemadroptableresponse"></a>
<a id="schema_DropTableResponse"></a>
<a id="tocSdroptableresponse"></a>
<a id="tocsdroptableresponse"></a>

```json
{
  "dropped": true,
  "purged": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|dropped|boolean|false|none|true if the table was found and removed from the catalog|
|purged|boolean|false|none|whether the underlying data was purged or is being purged|

<h2 id="tocS_Namespace">Namespace</h2>
<!-- backwards compatibility -->
<a id="schemanamespace"></a>
<a id="schema_Namespace"></a>
<a id="tocSnamespace"></a>
<a id="tocsnamespace"></a>

```json
[
  "accounting",
  "tax"
]

```

Reference to one or more levels of a namespace

### Properties

*None*

<h2 id="tocS_GetNamespaceResponse">GetNamespaceResponse</h2>
<!-- backwards compatibility -->
<a id="schemagetnamespaceresponse"></a>
<a id="schema_GetNamespaceResponse"></a>
<a id="tocSgetnamespaceresponse"></a>
<a id="tocsgetnamespaceresponse"></a>

```json
{
  "namespace": [
    "accounting",
    "tax"
  ],
  "properties": {
    "owner": "Ralph",
    "transient_lastDdlTime": "1452120468"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|[Namespace](#schemanamespace)|false|none|Reference to one or more levels of a namespace|
|properties|object|false|none|none|

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

<h2 id="tocS_TableAlreadyExistsError">TableAlreadyExistsError</h2>
<!-- backwards compatibility -->
<a id="schematablealreadyexistserror"></a>
<a id="schema_TableAlreadyExistsError"></a>
<a id="tocStablealreadyexistserror"></a>
<a id="tocstablealreadyexistserror"></a>

```json
{
  "message": "string",
  "type": "NoSuchNamespaceException",
  "code": 404,
  "metadata": {}
}

```

the given table identifier already exists and cannot be created / renamed to

### Properties

*None*

