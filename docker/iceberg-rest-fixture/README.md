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


## Build the Docker Image

When making changes to the local files and test them out, you can build the image locally:

```bash
# Build the project from iceberg root directory
./gradlew :iceberg-open-api:shadowJar

# Rebuild the docker image
docker image rm -f apache/iceberg-rest-fixture && docker build -t apache/iceberg-rest-fixture -f docker/iceberg-rest-fixture/Dockerfile .
```

## Configuration

The Docker image supports various environment variables for configuration:

### Catalog Configuration
- `CATALOG_CATALOG__IMPL` - Catalog implementation class
- `CATALOG_URI` - Database URI for JDBC catalogs
- `CATALOG_JDBC_USER` - Database username
- `CATALOG_JDBC_PASSWORD` - Database password
- `CATALOG_JDBC_STRICT__MODE` - Enable strict mode for JDBC

### Logging Configuration
- `CATALOG_LOG4J_LOGLEVEL` - Set the root logger level (e.g., WARN, ERROR, INFO)
- `CATALOG_LOG4J_CONFIG_FILE` - Path to custom log4j.properties file
- `CATALOG_LOG4J_LOGGER_<logger_name>` - Set specific logger levels

### Examples

```bash
# Reduce log verbosity to WARN level
docker run -e CATALOG_LOG4J_LOGLEVEL=WARN apache/iceberg-rest-fixture

# Set specific loggers to ERROR level to reduce noise
docker run \
  -e CATALOG_LOG4J_LOGLEVEL=WARN \
  -e CATALOG_LOG4J_LOGGER_ORG_APACHE_ICEBERG_BASEMETASTORECATALOG=ERROR \
  -e CATALOG_LOG4J_LOGGER_ORG_APACHE_ICEBERG_BASEMETASTORETABLEOPERATIONS=ERROR \
  apache/iceberg-rest-fixture

# Use custom log4j configuration file
docker run -e CATALOG_LOG4J_CONFIG_FILE=/custom/log4j.properties apache/iceberg-rest-fixture
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


