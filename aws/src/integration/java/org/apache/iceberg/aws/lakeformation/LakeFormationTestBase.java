/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws.lakeformation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AssumeRoleAwsClientFactory;
import org.apache.iceberg.aws.AwsIntegTestUtil;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.AttachRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.CreatePolicyRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.DeletePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.DetachRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.GetPolicyVersionRequest;
import software.amazon.awssdk.services.iam.model.GetRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.NoSuchEntityException;
import software.amazon.awssdk.services.iam.model.PolicyVersion;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.LakeFormationClientBuilder;
import software.amazon.awssdk.services.lakeformation.model.AlreadyExistsException;
import software.amazon.awssdk.services.lakeformation.model.CatalogResource;
import software.amazon.awssdk.services.lakeformation.model.DataLakePrincipal;
import software.amazon.awssdk.services.lakeformation.model.DataLakeSettings;
import software.amazon.awssdk.services.lakeformation.model.DataLocationResource;
import software.amazon.awssdk.services.lakeformation.model.DatabaseResource;
import software.amazon.awssdk.services.lakeformation.model.DeregisterResourceRequest;
import software.amazon.awssdk.services.lakeformation.model.DescribeResourceRequest;
import software.amazon.awssdk.services.lakeformation.model.EntityNotFoundException;
import software.amazon.awssdk.services.lakeformation.model.GetDataLakeSettingsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetDataLakeSettingsResponse;
import software.amazon.awssdk.services.lakeformation.model.GrantPermissionsRequest;
import software.amazon.awssdk.services.lakeformation.model.Permission;
import software.amazon.awssdk.services.lakeformation.model.PutDataLakeSettingsRequest;
import software.amazon.awssdk.services.lakeformation.model.PutDataLakeSettingsResponse;
import software.amazon.awssdk.services.lakeformation.model.RegisterResourceRequest;
import software.amazon.awssdk.services.lakeformation.model.Resource;
import software.amazon.awssdk.services.lakeformation.model.TableResource;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

@SuppressWarnings({"VisibilityModifier", "HideUtilityClassConstructor"})
public class LakeFormationTestBase {

  static final Logger LOG = LoggerFactory.getLogger(LakeFormationTestBase.class);
  static final int IAM_PROPAGATION_DELAY = 10000;
  static final int ASSUME_ROLE_SESSION_DURATION = 3600;
  static final String LF_REGISTER_PATH_ROLE_PREFIX = "LFRegisterPathRole_";
  static final String LF_PRIVILEGED_ROLE_PREFIX = "LFPrivilegedRole_";
  static final String LF_TEST_DB_PREFIX = "lf_test_db";
  static final String LF_TEST_TABLE_PREFIX = "lf_test_table";
  static final String TEST_PATH_PREFIX = "iceberg-lf-test/";
  static final String DEFAULT_IAM_POLICY_VERSION = "v1";
  static final String LF_AUTHORIZED_CALLER_VALUE = "emr";
  static final String LF_REGISTER_PATH_ROLE_S3_POLICY_PREFIX = "LFRegisterPathRoleS3Policy_";
  static final String LF_REGISTER_PATH_ROLE_LF_POLICY_PREFIX = "LFRegisterPathRoleLfPolicy_";
  static final String LF_REGISTER_PATH_ROLE_IAM_POLICY_PREFIX = "LFRegisterPathRoleIamPolicy_";
  static final String LF_PRIVILEGED_ROLE_POLICY_PREFIX = "LFPrivilegedRoleTestPolicy_";

  static Map<String, String> assumeRoleProperties;
  static String lfRegisterPathRoleName;
  static String lfPrivilegedRoleName;
  static String lfRegisterPathRoleArn;
  static String lfPrivilegedRoleArn;
  static String lfRegisterPathRoleS3PolicyName;
  static String lfRegisterPathRoleLfPolicyName;
  static String lfRegisterPathRoleIamPolicyName;
  static String lfPrivilegedRolePolicyName;
  static DataLakePrincipal principalUnderTest;
  static String testBucketPath =
      "s3://" + AwsIntegTestUtil.testBucketName() + "/" + TEST_PATH_PREFIX;
  static Schema schema =
      new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
  static PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).build();

  static GlueCatalog glueCatalogRegisterPathRole;
  static GlueCatalog glueCatalogPrivilegedRole;
  static IamClient iam;
  static LakeFormationClient lakeformation;
  static GlueClient glue;

  @BeforeAll
  public static void beforeClass() throws Exception {
    lfRegisterPathRoleName = LF_REGISTER_PATH_ROLE_PREFIX + UUID.randomUUID().toString();
    lfPrivilegedRoleName = LF_PRIVILEGED_ROLE_PREFIX + UUID.randomUUID().toString();
    lfRegisterPathRoleS3PolicyName =
        LF_REGISTER_PATH_ROLE_S3_POLICY_PREFIX + UUID.randomUUID().toString();
    lfRegisterPathRoleLfPolicyName =
        LF_REGISTER_PATH_ROLE_LF_POLICY_PREFIX + UUID.randomUUID().toString();
    lfRegisterPathRoleIamPolicyName =
        LF_REGISTER_PATH_ROLE_IAM_POLICY_PREFIX + UUID.randomUUID().toString();
    lfPrivilegedRolePolicyName = LF_PRIVILEGED_ROLE_POLICY_PREFIX + UUID.randomUUID().toString();

    iam =
        IamClient.builder()
            .region(Region.AWS_GLOBAL)
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .build();

    CreateRoleResponse response =
        iam.createRole(
            CreateRoleRequest.builder()
                .roleName(lfRegisterPathRoleName)
                .assumeRolePolicyDocument(
                    "{"
                        + "\"Version\":\"2012-10-17\","
                        + "\"Statement\":[{"
                        + "\"Effect\":\"Allow\","
                        + "\"Principal\":{"
                        + "\"Service\":[\"glue.amazonaws.com\","
                        + "\"lakeformation.amazonaws.com\"],"
                        + "\"AWS\":\"arn:aws:iam::"
                        + AwsIntegTestUtil.testAccountId()
                        + ":root\"},"
                        + "\"Action\": [\"sts:AssumeRole\"]}]}")
                .maxSessionDuration(ASSUME_ROLE_SESSION_DURATION)
                .build());
    lfRegisterPathRoleArn = response.role().arn();

    // create and attach test policy to lfRegisterPathRole
    createAndAttachRolePolicy(
        createPolicyArn(lfRegisterPathRoleS3PolicyName),
        lfRegisterPathRoleS3PolicyName,
        lfRegisterPathRolePolicyDocForS3(),
        lfRegisterPathRoleName);
    createAndAttachRolePolicy(
        createPolicyArn(lfRegisterPathRoleLfPolicyName),
        lfRegisterPathRoleLfPolicyName,
        lfRegisterPathRolePolicyDocForLakeFormation(),
        lfRegisterPathRoleName);
    createAndAttachRolePolicy(
        createPolicyArn(lfRegisterPathRoleIamPolicyName),
        lfRegisterPathRoleIamPolicyName,
        lfRegisterPathRolePolicyDocForIam(lfRegisterPathRoleArn),
        lfRegisterPathRoleName);
    waitForIamConsistency(lfRegisterPathRoleName, lfRegisterPathRoleIamPolicyName);

    // create lfPrivilegedRole
    response =
        iam.createRole(
            CreateRoleRequest.builder()
                .roleName(lfPrivilegedRoleName)
                .assumeRolePolicyDocument(
                    "{"
                        + "\"Version\":\"2012-10-17\","
                        + "\"Statement\":[{"
                        + "\"Effect\":\"Allow\","
                        + "\"Principal\":{"
                        + "\"AWS\":\"arn:aws:iam::"
                        + AwsIntegTestUtil.testAccountId()
                        + ":root\"},"
                        + "\"Action\": [\"sts:AssumeRole\","
                        + "\"sts:TagSession\"]}]}")
                .maxSessionDuration(ASSUME_ROLE_SESSION_DURATION)
                .build());
    lfPrivilegedRoleArn = response.role().arn();
    principalUnderTest =
        DataLakePrincipal.builder().dataLakePrincipalIdentifier(lfPrivilegedRoleArn).build();

    // create and attach test policy to lfPrivilegedRole
    createAndAttachRolePolicy(
        createPolicyArn(lfPrivilegedRolePolicyName),
        lfPrivilegedRolePolicyName,
        lfPrivilegedRolePolicyDoc(),
        lfPrivilegedRoleName);
    waitForIamConsistency(lfPrivilegedRoleName, lfPrivilegedRolePolicyName);

    // build lf and glue client with lfRegisterPathRole
    lakeformation =
        buildLakeFormationClient(lfRegisterPathRoleArn, "test_lf", AwsIntegTestUtil.testRegion());
    glue = buildGlueClient(lfRegisterPathRoleArn, "test_lf", AwsIntegTestUtil.testRegion());

    // put lf data lake settings
    GetDataLakeSettingsResponse getDataLakeSettingsResponse =
        lakeformation.getDataLakeSettings(GetDataLakeSettingsRequest.builder().build());
    PutDataLakeSettingsResponse putDataLakeSettingsResponse =
        lakeformation.putDataLakeSettings(
            putDataLakeSettingsRequest(
                lfRegisterPathRoleArn, getDataLakeSettingsResponse.dataLakeSettings(), true));

    // Build test glueCatalog with lfPrivilegedRole
    glueCatalogPrivilegedRole = new GlueCatalog();
    assumeRoleProperties = Maps.newHashMap();
    assumeRoleProperties.put("warehouse", "s3://path");
    assumeRoleProperties.put(
        AwsProperties.CLIENT_ASSUME_ROLE_REGION, AwsIntegTestUtil.testRegion());
    assumeRoleProperties.put(AwsProperties.GLUE_LAKEFORMATION_ENABLED, "true");
    assumeRoleProperties.put(AwsProperties.GLUE_ACCOUNT_ID, AwsIntegTestUtil.testAccountId());
    assumeRoleProperties.put(
        HttpClientProperties.CLIENT_TYPE, HttpClientProperties.CLIENT_TYPE_APACHE);
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, lfPrivilegedRoleArn);
    assumeRoleProperties.put(
        AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX
            + LakeFormationAwsClientFactory.LF_AUTHORIZED_CALLER,
        LF_AUTHORIZED_CALLER_VALUE);
    glueCatalogPrivilegedRole.initialize("test_privileged", assumeRoleProperties);

    // Build test glueCatalog with lfRegisterPathRole
    assumeRoleProperties.put(AwsProperties.GLUE_LAKEFORMATION_ENABLED, "false");
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, lfRegisterPathRoleArn);
    assumeRoleProperties.remove(
        AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX
            + LakeFormationAwsClientFactory.LF_AUTHORIZED_CALLER);
    assumeRoleProperties.put(
        AwsProperties.CLIENT_FACTORY, AssumeRoleAwsClientFactory.class.getName());
    glueCatalogRegisterPathRole = new GlueCatalog();
    glueCatalogRegisterPathRole.initialize("test_registered", assumeRoleProperties);
    // register S3 test bucket path
    deregisterResource(testBucketPath);
    registerResource(testBucketPath);
  }

  @AfterAll
  public static void afterClass() {
    GetDataLakeSettingsResponse getDataLakeSettingsResponse =
        lakeformation.getDataLakeSettings(GetDataLakeSettingsRequest.builder().build());
    lakeformation.putDataLakeSettings(
        putDataLakeSettingsRequest(
            lfRegisterPathRoleArn, getDataLakeSettingsResponse.dataLakeSettings(), false));
    detachAndDeleteRolePolicy(
        createPolicyArn(lfRegisterPathRoleS3PolicyName), lfRegisterPathRoleName);
    detachAndDeleteRolePolicy(
        createPolicyArn(lfRegisterPathRoleLfPolicyName), lfRegisterPathRoleName);
    detachAndDeleteRolePolicy(
        createPolicyArn(lfRegisterPathRoleIamPolicyName), lfRegisterPathRoleName);
    iam.deleteRole(DeleteRoleRequest.builder().roleName(lfRegisterPathRoleName).build());
    detachAndDeleteRolePolicy(createPolicyArn(lfPrivilegedRolePolicyName), lfPrivilegedRoleName);
    iam.deleteRole(DeleteRoleRequest.builder().roleName(lfPrivilegedRoleName).build());
    deregisterResource(testBucketPath);
  }

  void grantDatabasePrivileges(String dbName, Permission... permissions) {
    Resource dbResource =
        Resource.builder().database(DatabaseResource.builder().name(dbName).build()).build();
    lakeformation.grantPermissions(
        GrantPermissionsRequest.builder()
            .principal(principalUnderTest)
            .resource(dbResource)
            .permissions(permissions)
            .build());
  }

  void grantDataPathPrivileges(String resourceLocation) {
    Resource dataLocationResource =
        Resource.builder()
            .dataLocation(
                DataLocationResource.builder()
                    .resourceArn(getArnForS3Location(resourceLocation))
                    .build())
            .build();
    lakeformation.grantPermissions(
        GrantPermissionsRequest.builder()
            .principal(principalUnderTest)
            .resource(dataLocationResource)
            .permissions(Permission.DATA_LOCATION_ACCESS)
            .build());
  }

  void lfRegisterPathRoleCreateDb(String dbName) {
    glueCatalogRegisterPathRole.createNamespace(Namespace.of(dbName));
  }

  void lfRegisterPathRoleDeleteDb(String dbName) {
    glueCatalogRegisterPathRole.dropNamespace(Namespace.of(dbName));
  }

  void lfRegisterPathRoleCreateTable(String dbName, String tableName) {
    glueCatalogRegisterPathRole.createTable(
        TableIdentifier.of(Namespace.of(dbName), tableName),
        schema,
        partitionSpec,
        getTableLocation(tableName),
        null);
  }

  void lfRegisterPathRoleDeleteTable(String dbName, String tableName) {
    glueCatalogRegisterPathRole.dropTable(
        TableIdentifier.of(Namespace.of(dbName), tableName), false);
  }

  String getTableLocation(String tableName) {
    return testBucketPath + tableName;
  }

  void grantCreateDbPermission() {
    lakeformation.grantPermissions(
        GrantPermissionsRequest.builder()
            .principal(principalUnderTest)
            .permissions(Permission.CREATE_DATABASE)
            .resource(Resource.builder().catalog(CatalogResource.builder().build()).build())
            .build());
  }

  void grantTablePrivileges(String dbName, String tableName, Permission... tableDdlPrivileges) {
    Resource tableResource =
        Resource.builder()
            .table(TableResource.builder().databaseName(dbName).name(tableName).build())
            .build();
    GrantPermissionsRequest grantDataLakePrivilegesRequest =
        GrantPermissionsRequest.builder()
            .principal(principalUnderTest)
            .resource(tableResource)
            .permissionsWithGrantOption(tableDdlPrivileges)
            .permissions(tableDdlPrivileges)
            .build();
    lakeformation.grantPermissions(grantDataLakePrivilegesRequest);
  }

  String getRandomDbName() {
    return LF_TEST_DB_PREFIX + UUID.randomUUID().toString().replace("-", "");
  }

  String getRandomTableName() {
    return LF_TEST_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
  }

  private static void waitForIamConsistency(String roleName, String policyName) {
    // wait to make sure IAM up to date
    Awaitility.await()
        .pollDelay(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertThat(
                        iam.getRolePolicy(
                            GetRolePolicyRequest.builder()
                                .roleName(roleName)
                                .policyName(policyName)
                                .build()))
                    .isNotNull());
  }

  private static LakeFormationClient buildLakeFormationClient(
      String roleArn, String sessionName, String region) {
    AssumeRoleRequest request =
        AssumeRoleRequest.builder()
            .roleArn(roleArn)
            .roleSessionName(sessionName)
            .durationSeconds(ASSUME_ROLE_SESSION_DURATION)
            .build();

    LakeFormationClientBuilder clientBuilder = LakeFormationClient.builder();

    clientBuilder.credentialsProvider(
        StsAssumeRoleCredentialsProvider.builder()
            .stsClient(
                StsClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build())
            .refreshRequest(request)
            .build());

    clientBuilder.region(Region.of(region));
    clientBuilder.httpClientBuilder(UrlConnectionHttpClient.builder());
    return clientBuilder.build();
  }

  private static GlueClient buildGlueClient(String roleArn, String sessionName, String region) {
    AssumeRoleRequest request =
        AssumeRoleRequest.builder()
            .roleArn(roleArn)
            .roleSessionName(sessionName)
            .durationSeconds(ASSUME_ROLE_SESSION_DURATION)
            .build();

    GlueClientBuilder clientBuilder = GlueClient.builder();

    clientBuilder.credentialsProvider(
        StsAssumeRoleCredentialsProvider.builder()
            .stsClient(
                StsClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build())
            .refreshRequest(request)
            .build());

    clientBuilder.region(Region.of(region));
    clientBuilder.httpClientBuilder(UrlConnectionHttpClient.builder());
    return clientBuilder.build();
  }

  private static void registerResource(String s3Location) {
    String arn = getArnForS3Location(s3Location);
    try {
      lakeformation.registerResource(
          RegisterResourceRequest.builder()
              .resourceArn(arn)
              .roleArn(lfRegisterPathRoleArn)
              .useServiceLinkedRole(false)
              .build());
      // when a resource is registered, LF will update SLR with necessary permissions which has a
      // propagation delay
      Awaitility.await()
          .pollDelay(Duration.ofSeconds(1))
          .atMost(Duration.ofSeconds(10))
          .ignoreExceptions()
          .untilAsserted(
              () ->
                  assertThat(
                          lakeformation
                              .describeResource(
                                  DescribeResourceRequest.builder().resourceArn(arn).build())
                              .resourceInfo()
                              .roleArn())
                      .isEqualToIgnoringCase(lfRegisterPathRoleArn));
    } catch (AlreadyExistsException e) {
      LOG.warn("Resource {} already registered. Error: {}", arn, e.getMessage(), e);
    } catch (Exception e) {
      // ignore exception
    }
  }

  private static void deregisterResource(String s3Location) {
    String arn = getArnForS3Location(s3Location);
    try {
      lakeformation.deregisterResource(
          DeregisterResourceRequest.builder().resourceArn(arn).build());
    } catch (EntityNotFoundException e) {
      LOG.info("Resource {} not found. Error: {}", arn, e.getMessage(), e);
    }
  }

  private static String createPolicyArn(String policyName) {
    return String.format(
        "arn:%s:iam::%s:policy/%s",
        PartitionMetadata.of(Region.of(AwsIntegTestUtil.testRegion())).id(),
        AwsIntegTestUtil.testAccountId(),
        policyName);
  }

  private static void createAndAttachRolePolicy(
      String policyArn, String policyName, String policyDocument, String roleName) {
    createOrReplacePolicy(policyArn, policyName, policyDocument, roleName);
    attachRolePolicyIfNotExists(policyArn, policyName, roleName);
  }

  private static void attachRolePolicyIfNotExists(
      String policyArn, String policyName, String roleName) {
    try {
      iam.getRolePolicy(
          GetRolePolicyRequest.builder().roleName(roleName).policyName(policyName).build());
      LOG.info("Policy {} already attached to role {}", policyName, roleName);
    } catch (NoSuchEntityException e) {
      LOG.info("Attaching policy {} to role {}", policyName, roleName, e);
      iam.attachRolePolicy(
          AttachRolePolicyRequest.builder().roleName(roleName).policyArn(policyArn).build());
    }
  }

  private static void createOrReplacePolicy(
      String policyArn, String policyName, String policyDocument, String roleName) {
    try {
      PolicyVersion existingPolicy =
          iam.getPolicyVersion(
                  GetPolicyVersionRequest.builder()
                      .policyArn(policyArn)
                      .versionId(DEFAULT_IAM_POLICY_VERSION)
                      .build())
              .policyVersion();
      String currentDocument =
          URLDecoder.decode(existingPolicy.document(), StandardCharsets.UTF_8.name());
      if (Objects.equals(currentDocument, policyDocument)) {
        LOG.info(
            "Policy {} already exists and policy content did not change. Nothing to do.",
            policyArn);
      } else {
        LOG.info(
            "Role policy exists but has different policy content. Existing content: {}, new content: {}",
            currentDocument,
            policyDocument);
        detachAndDeleteRolePolicy(policyArn, roleName);
        createPolicy(policyName, policyDocument);
      }
    } catch (NoSuchEntityException e) {
      createPolicy(policyName, policyDocument);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static void createPolicy(String policyName, String policyDocument) {
    LOG.info("Creating policy {} with version v1", policyName);
    iam.createPolicy(
        CreatePolicyRequest.builder()
            .policyName(policyName)
            .policyDocument(policyDocument)
            .build());
  }

  private static void detachAndDeleteRolePolicy(String policyArn, String roleName) {
    LOG.info("Detaching role policy {} if attached", policyArn);
    try {
      iam.detachRolePolicy(
          DetachRolePolicyRequest.builder().policyArn(policyArn).roleName(roleName).build());
    } catch (NoSuchEntityException ex) {
      // nothing to do if it doesn't exist
    }

    LOG.info("Deleting role policy : {} if already exists", policyArn);
    try {
      iam.deletePolicy(DeletePolicyRequest.builder().policyArn(policyArn).build());
    } catch (NoSuchEntityException e) {
      // nothing to do if it doesn't exist
    }
  }

  private static String lfRegisterPathRolePolicyDocForS3() {
    return "{"
        + "\"Version\":\"2012-10-17\","
        + "\"Statement\":[{"
        + "\"Effect\":\"Allow\","
        + "\"Action\": [\"s3:*\"],"
        + "\"Resource\": [\"*\"]}]}";
  }

  private static String lfRegisterPathRolePolicyDocForLakeFormation() {
    return "{"
        + "\"Version\":\"2012-10-17\","
        + "\"Statement\":[{"
        + "\"Sid\":\"policy1\","
        + "\"Effect\":\"Allow\","
        + "\"Action\":[\"lakeformation:GetDataLakeSettings\","
        + "\"lakeformation:PutDataLakeSettings\","
        + "\"lakeformation:GrantPermissions\","
        + "\"lakeformation:RevokePermissions\","
        + "\"lakeformation:RegisterResource\","
        + "\"lakeformation:DeregisterResource\","
        + "\"lakeformation:GetDataAccess\","
        + "\"glue:CreateDatabase\",\"glue:DeleteDatabase\","
        + "\"glue:Get*\", \"glue:CreateTable\", \"glue:DeleteTable\", \"glue:UpdateTable\"],"
        + "\"Resource\":[\"*\"]}]}";
  }

  private static String lfRegisterPathRolePolicyDocForIam(String roleArn) {
    return "{\n"
        + "\"Version\":\"2012-10-17\","
        + "\"Statement\":{"
        + "\"Effect\":\"Allow\","
        + "\"Action\": ["
        + "\"iam:PassRole\","
        + "\"iam:GetRole\""
        + "],"
        + "\"Resource\": ["
        + "\""
        + roleArn
        + "\""
        + "]}}";
  }

  private static String lfPrivilegedRolePolicyDoc() {
    return "{"
        + "\"Version\":\"2012-10-17\","
        + "\"Statement\":[{"
        + "\"Sid\":\"policy1\","
        + "\"Effect\":\"Allow\","
        + "\"Action\":[\"glue:CreateDatabase\", \"glue:DeleteDatabase\","
        + "\"glue:Get*\", \"glue:UpdateTable\", \"glue:DeleteTable\", \"glue:CreateTable\","
        + "\"lakeformation:GetDataAccess\"],"
        + "\"Resource\":[\"*\"]}]}";
  }

  private static PutDataLakeSettingsRequest putDataLakeSettingsRequest(
      String adminArn, DataLakeSettings dataLakeSettings, boolean add) {
    List<DataLakePrincipal> dataLakeAdmins = Lists.newArrayList(dataLakeSettings.dataLakeAdmins());
    if (add) {
      dataLakeAdmins.add(DataLakePrincipal.builder().dataLakePrincipalIdentifier(adminArn).build());
    } else {
      dataLakeAdmins.removeIf(p -> p.dataLakePrincipalIdentifier().equals(adminArn));
    }
    DataLakeSettings newDataLakeSettings =
        DataLakeSettings.builder()
            .dataLakeAdmins(dataLakeAdmins)
            .allowExternalDataFiltering(true)
            .externalDataFilteringAllowList(
                DataLakePrincipal.builder()
                    .dataLakePrincipalIdentifier(AwsIntegTestUtil.testAccountId())
                    .build())
            .authorizedSessionTagValueList(LF_AUTHORIZED_CALLER_VALUE)
            .build();

    return PutDataLakeSettingsRequest.builder().dataLakeSettings(newDataLakeSettings).build();
  }

  private static String getArnForS3Location(String s3Location) {
    return s3Location.replace("s3://", "arn:aws:s3:::");
  }
}
