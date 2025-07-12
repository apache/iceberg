---
title: "BladePipe"
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

# BladePipe

[BladePipe](https://www.bladepipe.com/) is a real-time end-to-end data integration tool, offering 40+ out-of-the-box connectors for analytics or AI. It allows to move data faster and easier than ever, with ultra-low latency less than 3 seconds. It provides a one-stop data movement solution, including schema evolution, data migration and sync, verification and correction, monitoring and alerting.

## Supported Sources
Now BladePipe supports data integration to Iceberg from the following sources:

- MySQL/MariaDB/AuroraMySQL
- Oracle
- PostgreSQL
- SQL Server
- Kafka

More sources are to be supported.

## Supported Catalogs and Storage
BladePipe currently supports 3 catalogs and 2 object storage:

- AWS Glue + AWS S3
- Nessie + MinIO / AWS S3
- REST Catalog + MinIO / AWS S3


## Getting Started
In this article, we will show how to load data from MySQL (self-hosted) to Iceberg (AWS Glue + S3).

### 1. Download and Run BladePipe
Follow the instructions in [Install Worker (Docker)](https://doc.bladepipe.com/productOP/byoc/installation/install_worker_docker) or [Install Worker (Binary)](https://doc.bladepipe.com/productOP/byoc/installation/install_worker_binary) to download and install a BladePipe Worker.

**Note**: Alternatively, you can choose to deploy and run [BladePipe Enterprise](https://doc.bladepipe.com/productOP/onPremise/installation/install_all_in_one_binary).

### 2. Add DataSources

1. Log in to the [BladePipe Cloud](https://cloud.bladepipe.com).
2. Click **DataSource** > **Add DataSource**.
3. Add a MySQL instance and an Iceberg instance. For Iceberg, fill in the following content (replace `<...>` with your values):
    - **Address**: Fill in the AWS Glue endpoint.
    
    ```text
    glue.<aws_glue_region_code>.amazonaws.com
    ```
    
    - **Version**: Leave as default.
    - **Description**: Fill in meaningful words to help identify it.
    - **Extra Info**:
        - **httpsEnabled**: Enable it to set the value as true.
        - **catalogName**: Enter a meaningful name, such as glue_<biz_name>_catalog.
        - **catalogType**: Fill in `GLUE`.
        - **catalogWarehouse**: The place where metadata and files are stored, such as s3://<biz_name>_iceberg.
        - **catalogProps**:    
     
    ```json
    {
    "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "s3.endpoint": "https://s3.<aws_s3_region_code>.amazonaws.com",
    "s3.access-key-id": "<aws_s3_iam_user_access_key>",
    "s3.secret-access-key": "<aws_s3_iam_user_secret_key>",
    "s3.path-style-access": "true",
    "client.region": "<aws_s3_region>",
    "client.credentials-provider.glue.access-key-id": "<aws_glue_iam_user_access_key>",
    "client.credentials-provider.glue.secret-access-key": "<aws_glue_iam_user_secret_key>",
    "client.credentials-provider": "com.amazonaws.glue.catalog.credentials.GlueAwsCredentialsProvider"
    }
    ```

    ![Iceberg configuration](https://doc.bladepipe.com/assets/images/1-afd4d16b1739f59151ceb30d6189cfc4.png)   
    See [Add an Iceberg DataSource](https://doc.bladepipe.com/dataMigrationAndSync/datasource_func/Iceberg/props_for_iceberg_ds) for more details.

### 3. Create a DataJob
1. Go to **DataJob** > [**Create DataJob**](https://doc.bladepipe.com/operation/job_manage/create_job/create_full_incre_task).
2. Select the source and target DataSources, and click **Test Connection** for both. Here's the recommended Iceberg structure configuration:  
   ```json
   {
     "format-version": "2",
     "parquet.compression": "snappy",
     "iceberg.write.format": "parquet",
     "write.metadata.delete-after-commit.enabled": "true",
     "write.metadata.previous-versions-max": "3",
     "write.update.mode": "merge-on-read",
     "write.delete.mode": "merge-on-read",
     "write.merge.mode": "merge-on-read",
     "write.distribution-mode": "hash",
     "write.object-storage.enabled": "true",
     "write.spark.accept-any-schema": "true"
   }
   ```
    ![Iceberg structure configuration](https://doc.bladepipe.com/assets/images/2-e436c11d029481dc58c5a86d17a2fc7b.png)

3. Select **Incremental** for DataJob Type, together with the **Full Data** option.
  ![DataJob Type](https://doc.bladepipe.com/assets/images/3-aaf4ce14be8ce88cbcdb85c426ceab33.png)

4. Select the tables to be replicated.
  ![Select tables](https://doc.bladepipe.com/assets/images/4-a04b97e30e5784d7159c6a90e948cdbd.png)

5. Select the columns to be replicated.
  ![Select columns](https://media.licdn.com/dms/image/v2/D5612AQFBsiFdkGVeqg/article-inline_image-shrink_1500_2232/B56ZcV9LnIHoAU-/0/1748420052594?e=1757548800&v=beta&t=Fti1XONv402KnO6Zth8Z4-Og-CZgrnUoJAa_p5-kpKw)

6. Confirm the DataJob creation, and start to run the DataJob.
  ![mysql_to_iceberg_running](https://doc.bladepipe.com/assets/images/6-3a09842159318f4b02a02b13b575f071.png)
