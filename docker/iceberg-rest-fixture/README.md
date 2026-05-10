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

# Iceberg REST Catalog Adapter Test Fixture

For converting different catalog implementations into a rest one.
Adapter for wrapping the existing catalog backends over REST.


## Configuration

All configuration is provided via environment variables.

### Backend catalog properties

Catalog properties can be set via `CATALOG_*` environment variables. The
`CATALOG_` prefix is stripped; single underscores become dots (`.`); double
underscores become dashes (`-`). Names are lowercased.

| Env var | Catalog property |
|---|---|
| `CATALOG_CATALOG_NAME` | `catalog.name` |
| `CATALOG_WAREHOUSE` | `warehouse` |
| `CATALOG_URI` | `uri` |
| `CATALOG_CATALOG__IMPL` | `catalog-impl` |
| `CATALOG_IO__IMPL` | `io-impl` |
| `CATALOG_JDBC_USER` | `jdbc.user` |

If `catalog-impl` and `uri` are unset, the fixture defaults to an in-memory
SQLite `JdbcCatalog`.

### Catalog name

By default, the fixture serves a catalog named `rest_backend`. To match a
name expected by a specific engine (for example, a catalog created via Trino
or PyIceberg), override the `catalog.name` property:

```bash
docker run -e CATALOG_CATALOG_NAME=mycatalog -p 8181:8181 apache/iceberg-rest-fixture
```


## Build the Docker Image

When making changes to the local files and test them out, you can build the image locally:

```bash
# Build the project from iceberg root directory
./gradlew :iceberg-open-api:shadowJar

# Rebuild the docker image
docker image rm -f apache/iceberg-rest-fixture && docker build -t apache/iceberg-rest-fixture -f docker/iceberg-rest-fixture/Dockerfile .
```

## Browse

To browse the catalog, you can use `pyiceberg`:

```
➜  ~ pyiceberg --uri http://localhost:8181 list 
default             
nyc                 
ride_sharing_dataset
➜  ~ pyiceberg --uri http://localhost:8181 list nyc
nyc.taxis           
nyc.taxis3          
nyc.taxis4          
nyc.taxis_copy_maybe
nyc.taxis_skeleton  
nyc.taxis_skeleton2 
➜  ~ pyiceberg --uri http://localhost:8181 describe --entity=table tpcds_iceberg.customer 
Table format version  2                                                                                                                                                         
Metadata location     s3://iceberg-test-data/tpc/tpc-ds/3.2.0/1000/iceberg/customer/metadata/00001-1bccfcc4-69f6-4505-8df5-4de78356e327.metadata.json                           
Table UUID            dce215f7-6301-4a73-acc4-6e12db016abb                                                                                                                      
Last Updated          1653550004061                                                                                                                                             
Partition spec        []                                                                                                                                                        
Sort order            []                                                                                                                                                        
Schema                Schema                                                                                                                                                    
                      ├── 1: c_customer_sk: optional int                                                                                                                        
                      ├── 2: c_customer_id: optional string                                                                                                                     
                      ├── 3: c_current_cdemo_sk: optional int                                                                                                                   
                      ├── 4: c_current_hdemo_sk: optional int                                                                                                                   
                      ├── 5: c_current_addr_sk: optional int                                                                                                                    
                      ├── 6: c_first_shipto_date_sk: optional int                                                                                                               
                      ├── 7: c_first_sales_date_sk: optional int                                                                                                                
                      ├── 8: c_salutation: optional string                                                                                                                      
                      ├── 9: c_first_name: optional string                                                                                                                      
                      ├── 10: c_last_name: optional string                                                                                                                      
                      ├── 11: c_preferred_cust_flag: optional string                                                                                                            
                      ├── 12: c_birth_day: optional int                                                                                                                         
                      ├── 13: c_birth_month: optional int                                                                                                                       
                      ├── 14: c_birth_year: optional int                                                                                                                        
                      ├── 15: c_birth_country: optional string                                                                                                                  
                      ├── 16: c_login: optional string                                                                                                                          
                      ├── 17: c_email_address: optional string                                                                                                                  
                      └── 18: c_last_review_date: optional string                                                                                                               
Snapshots             Snapshots                                                                                                                                                 
                      └── Snapshot 0: s3://iceberg-test-data/tpc/tpc-ds/3.2.0/1000/iceberg/customer/metadata/snap-643656366840285027-1-5ce13497-7330-4d02-8206-7e313e43209c.avro
Properties            write.object-storage.enabled  true                                                                                                                        
                      write.object-storage.path     s3://iceberg-test-data/tpc/tpc-ds/3.2.0/1000/iceberg/customer/data
```


