package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.OrgStatus;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import scala.concurrent.duration.FiniteDuration;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class, ElasticSearchUtil.class})
@PowerMockIgnore("javax.management.*")
public class OrganisationManagementActorTest {

  private static ActorSystem system;
  private static CassandraOperation cassandraOperation;
  private static final Props props = Props.create(OrganisationManagementActor.class);
  private static String orgTypeId1 = "";
  private static String orgTypeId2 = "";
  private static String orgId = "";
  private static String addressId = "";
  private static final String source = "Test";
  private static final String HASH_TAG_ID = "someHashtagId";
  private static final String LOCATION_ID = "someLocationId";
  private static final String EXTERNAL_ID = "someExternalId";
  private static final String ORG_NAME = "someOrgName";
  private static final String PROVIDER = "provider";
  private static final String CHANNEL = "channel";
  private static final String parentOrgId = "parentOrgId";
  private static final String USER_ID = "someUserId";
  private static final String VALID_EMAIL = "someEmail@someDomain.com";
  private static final String INVALID_EMAIL = "invalidEmail";
  private static final String ROOT_ORG_ID = "someRootOrgId";
  private static final long INVALID_STATUS = 10000;
  private static FiniteDuration duration = duration("10 second");

  @BeforeClass
  public static void setUp() {
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    system = ActorSystem.create("system");
  }

  @Before
  public void beforeEachTest() {
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    Mockito.reset(cassandraOperation);
  }

  @Test
  public void testCreateRootOrgWithUniqueChannelSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordsByProperty(true);
    mockCasandraInsertResponse();
    subject.tell(createOrgRequest(true, false, false, null), probe.getRef());
    Response response = probe.expectMsgClass(duration, Response.class);
    Assert.assertTrue(
        null != response && ((String) response.get(JsonKey.RESPONSE)).equals(JsonKey.SUCCESS));
  }

  @Test
  public void testCreateRootOrgWithExistingChannelFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordsByProperty(false);
    subject.tell(createOrgRequest(true, false, false, null), probe.getRef());
    ProjectCommonException exception = probe.expectMsgClass(duration, ProjectCommonException.class);
    Assert.assertTrue(
        null != exception
            && exception.getResponseCode() == ResponseCode.CLIENT_ERROR.getResponseCode());
  }

  @Test
  public void testCreateSubOrgWithoutChannelSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraInsertResponse();
    subject.tell(createOrgRequest(false, false, false, null), probe.getRef());
    Response response = probe.expectMsgClass(duration, Response.class);
    Assert.assertTrue(
        null != response && ((String) response.get(JsonKey.RESPONSE)).equals(JsonKey.SUCCESS));
  }

  @Test
  public void testCreateSubOrgWithExistingChannelSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordsByProperties(false);
    mockCasandraInsertResponse();
    subject.tell(createOrgRequest(false, true, false, null), probe.getRef());
    Response response = probe.expectMsgClass(duration, Response.class);
    Assert.assertTrue(
        null != response && ((String) response.get(JsonKey.RESPONSE)).equals(JsonKey.SUCCESS));
  }

  @Test
  public void testCreateSubOrgWithNonExistingChannelFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordsByProperties(true);
    subject.tell(createOrgRequest(false, true, false, null), probe.getRef());
    ProjectCommonException exception = probe.expectMsgClass(duration, ProjectCommonException.class);
    Assert.assertTrue(
        null != exception
            && exception.getResponseCode() == ResponseCode.CLIENT_ERROR.getResponseCode());
  }

  @Test
  public void testCreateRootOrgWithValidEmailSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordsByProperty(true);
    mockCasandraInsertResponse();
    subject.tell(createOrgRequest(true, false, false, VALID_EMAIL), probe.getRef());
    Response response = probe.expectMsgClass(duration, Response.class);
    Assert.assertTrue(
        null != response && ((String) response.get(JsonKey.RESPONSE)).equals(JsonKey.SUCCESS));
  }

  @Test
  public void testCreateSubOrgWithValidEmailSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraInsertResponse();
    subject.tell(createOrgRequest(false, false, false, VALID_EMAIL), probe.getRef());
    Response response = probe.expectMsgClass(duration, Response.class);
    Assert.assertTrue(
        null != response && ((String) response.get(JsonKey.RESPONSE)).equals(JsonKey.SUCCESS));
  }

  @Test
  public void testCreateRootOrgWithInvalidEmailFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordsByProperty(false);
    subject.tell(createOrgRequest(true, false, false, INVALID_EMAIL), probe.getRef());
    ProjectCommonException exception = probe.expectMsgClass(duration, ProjectCommonException.class);
    Assert.assertTrue(
        null != exception
            && exception.getResponseCode() == ResponseCode.CLIENT_ERROR.getResponseCode());
  }

  @Test
  public void testCreateSubOrgWithInvalidEmailFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordsByProperties(false);
    subject.tell(createOrgRequest(false, false, false, INVALID_EMAIL), probe.getRef());
    ProjectCommonException exception = probe.expectMsgClass(duration, ProjectCommonException.class);
    Assert.assertTrue(
        null != exception
            && exception.getResponseCode() == ResponseCode.CLIENT_ERROR.getResponseCode());
  }

  @Test
  public void testCreateRootOrgWithUniqueExternalIdSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordsByProperty(true);
    mockCasandraInsertResponse();
    subject.tell(createOrgRequest(true, true, true, null), probe.getRef());
    Response response = probe.expectMsgClass(duration, Response.class);
    Assert.assertTrue(
        null != response && ((String) response.get(JsonKey.RESPONSE)).equals(JsonKey.SUCCESS));
  }

  @Test
  public void testCreateRootOrgWithDuplicateExternalIdFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordsByProperty(false);
    subject.tell(createOrgRequest(true, true, true, null), probe.getRef());
    ProjectCommonException exception = probe.expectMsgClass(duration, ProjectCommonException.class);
    Assert.assertTrue(
        null != exception
            && exception.getResponseCode() == ResponseCode.CLIENT_ERROR.getResponseCode());
  }

  @Test
  public void testCreateSubOrgWithUniqueExternalIdSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordsByProperty(false);
    mockCasandraInsertResponse();
    subject.tell(createOrgRequest(false, false, true, null), probe.getRef());
    Response response = probe.expectMsgClass(duration, Response.class);
    Assert.assertTrue(
        null != response && ((String) response.get(JsonKey.RESPONSE)).equals(JsonKey.SUCCESS));
  }

  @Test
  public void testInvalidOperation() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation("INVALID_OPERATION");
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exception = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exception);
  }

  @Test
  public void testInvalidMessageType() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell("Invalid Type", probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateOrgStatusToRetiresSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordById(false);
    mockCasandraUpdateRecordResponse();
    subject.tell(
        UpdateOrgStatusRequest(orgId, new BigInteger(String.valueOf(OrgStatus.RETIRED.getValue()))),
        probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateOrgStatusToInactiveSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordById(false);
    mockCasandraUpdateRecordResponse();
    subject.tell(
        UpdateOrgStatusRequest(
            orgId, new BigInteger(String.valueOf(OrgStatus.INACTIVE.getValue()))),
        probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateOrgStatusToBlockedSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordById(false);
    mockCasandraUpdateRecordResponse();
    subject.tell(
        UpdateOrgStatusRequest(orgId, new BigInteger(String.valueOf(OrgStatus.BLOCKED.getValue()))),
        probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateOrgStatusToActivesSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordById(false);
    mockCasandraUpdateRecordResponse();
    subject.tell(
        UpdateOrgStatusRequest(orgId, new BigInteger(String.valueOf(OrgStatus.ACTIVE.getValue()))),
        probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateOrgStatusWithInvalidOrgIdFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordById(true);
    subject.tell(
        UpdateOrgStatusRequest(orgId, new BigInteger(String.valueOf(OrgStatus.ACTIVE.getValue()))),
        probe.getRef());
    ProjectCommonException exception =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exception);
  }

  @Test
  public void testUpdateOrgStatusWithInvalidStatusFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraGetRecordById(false);
    mockCasandraUpdateRecordResponse();
    subject.tell(UpdateOrgStatusRequest(orgId, BigInteger.valueOf(INVALID_STATUS)), probe.getRef());
    ProjectCommonException exception =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exception);
  }

  @Test
  public void testUpdateStatusWithInvalidOrgIdFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, orgId + "ucic");
    orgMap.put(JsonKey.STATUS, new BigInteger(String.valueOf(OrgStatus.RETIRED.getValue())));
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateStatusWithInvalidStateTransitionFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, orgId);
    orgMap.put(JsonKey.STATUS, new BigInteger(String.valueOf(OrgStatus.RETIRED.getValue())));
    orgMap.put(JsonKey.STATUS, new BigInteger("10"));
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateStatusEx() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.PROVIDER, source);
    orgMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    orgMap.put(JsonKey.STATUS, new BigInteger("10"));
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateOrgExc1() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, "");
    orgMap.put("imgUrl", "test");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateOrgExcInvalidOrgId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, orgId + "bdu438f");
    orgMap.put("imgUrl", "test");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateOrgSuc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, orgId);
    orgMap.put("imgUrl", "test");
    // orgMap.put(JsonKey.ORG_TYPE, "ORG_TYPE_0002");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateOrgSuccessWithValidEmail() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, orgId);
    orgMap.put(JsonKey.EMAIL, VALID_EMAIL);
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUpdateOrgFailureWithInvalidEmail() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, orgId);
    orgMap.put(JsonKey.EMAIL, INVALID_EMAIL);
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateRootOrgSuccessWithValidEmail() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, ROOT_ORG_ID);
    orgMap.put(JsonKey.EMAIL, VALID_EMAIL);
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUpdateRootOrgFailureWithInvalidEmail() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, ROOT_ORG_ID);
    orgMap.put(JsonKey.EMAIL, INVALID_EMAIL);
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateOrgExc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, orgId);
    orgMap.put("imgUrl", "test");
    orgMap.put(JsonKey.PROVIDER, PROVIDER);
    orgMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateOrgSuc001() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    orgMap.put(JsonKey.PROVIDER, PROVIDER);
    orgMap.put(JsonKey.LOC_ID, LOCATION_ID);
    orgMap.put(JsonKey.PARENT_ORG_ID, parentOrgId);
    orgMap.put("imgUrl", "test");
    List<Map<String, Object>> contactDetails = new ArrayList<>();
    Map<String, Object> contactDetail = new HashMap<>();
    contactDetail.put("fax", "100");
    contactDetails.add(contactDetail);
    orgMap.put(JsonKey.CONTACT_DETAILS, contactDetails);
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateOrgSuc002() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.HASHTAGID, HASH_TAG_ID);
    orgMap.put("imgUrl", "test");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testCreateOrg002() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put(JsonKey.LOC_ID, "test");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testCreateOrg003() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put(JsonKey.ORG_TYPE, "skmdlfk");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetOrgSuc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_ORG_DETAILS.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response resp = probe.expectMsgClass(duration("200 second"), Response.class);
    try {
      addressId =
          (String)
              (((Map<String, Object>) resp.getResult().get(JsonKey.RESPONSE))
                  .get(JsonKey.ADDRESS_ID));
      Assert.assertTrue(null != addressId);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  @Test
  public void testGetOrgExc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_ORG_DETAILS.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, null);
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testAddMemberToOrgExc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, "");
    orgMap.put(JsonKey.USER_ID, "");
    orgMap.put(JsonKey.ROLE, "");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testAddMemberToOrgExc2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION, null);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testAddMemberToOrgSuc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, "");
    orgMap.put(JsonKey.USER_ID, "");
    orgMap.put(JsonKey.ROLE, "");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testAddMemberToOrgSuc001() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.USER_ID, USER_ID + "01");
    innerMap.put(JsonKey.ROLE, "content-reviewer");
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testAddMemberToOrgSuc001AddAgain() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.USER_ID, USER_ID + "01");
    List<String> roles = new ArrayList<>();
    roles.add("PUBLIC");
    innerMap.put(JsonKey.ROLES, roles);
    innerMap.put(JsonKey.ROLE, "content-creator");
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testAddMemberToOrgSuc001AddAgain2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.USER_ID, USER_ID + "01");
    innerMap.put(JsonKey.ROLE, "admin");
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void test27AddMemberToOrgSuc001AddAgain3() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.USER_ID, USER_ID + "01");
    innerMap.put(JsonKey.ROLE, "member");
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testAddMemberToOrgSuc001Exc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.USER_ID, USER_ID + "01");
    List<String> roles = new ArrayList<>();
    roles.add("TEST");
    innerMap.put(JsonKey.ROLES, roles);
    innerMap.put(JsonKey.ROLE, "CONTENT_CREATOR");
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testAddMemberToOrgExpUserIdNull() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.USER_ID, null);
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testAddMemberToOrgExpInvalidOrgId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId + "udb932d");
    innerMap.put(JsonKey.USER_ID, USER_ID + "01");
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testAddMemberToOrgExpInvalidUserId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.USER_ID, USER_ID + "01n49");
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testRemoveMemberFromOrgSuc001() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.USER_ID, USER_ID + "01");
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testRemoveMemberFromOrgExpNullOrgId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, null);
    innerMap.put(JsonKey.USER_ID, USER_ID + "01");
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testRemoveMemberFromOrgExpNullUsrId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.USER_ID, null);
    reqObj.getRequest().put(JsonKey.USER_ORG, innerMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testRemoveMemberFromOrgExpInvalidRequestData() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    innerMap.put(JsonKey.USER_ID, null);
    reqObj.getRequest().put(JsonKey.USER_ORG, null);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "user1");
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testRemoveMemberFromOrgExc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, "");
    orgMap.put(JsonKey.USER_ID, "");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testJoinMemberOrgSuc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, "");
    orgMap.put(JsonKey.USER_ID, "");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testJoinMemberOrgExc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, "");
    orgMap.put(JsonKey.USER_ID, "");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testApproveMemberOrgSuc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, "");
    orgMap.put(JsonKey.USER_ID, "");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testApproveMemberFromOrgExc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, "");
    orgMap.put(JsonKey.USER_ID, "");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testRejectMemberOrgSuc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, "");
    orgMap.put(JsonKey.USER_ID, "");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testRejectMemberOrgExc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, "");
    orgMap.put(JsonKey.USER_ID, "");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testCreateOrgType() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG_TYPE.getValue());
    reqObj.setRequestId(ExecutionContext.getRequestId());
    reqObj.setEnv(1);
    reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_0001");
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    assertEquals("SUCCESS", response.getResult().get(JsonKey.RESPONSE));
    Request req = new Request();
    req.setOperation(ActorOperations.GET_ORG_TYPE_LIST.getValue());
    req.setRequestId(ExecutionContext.getRequestId());
    req.setEnv(1);
    subject.tell(req, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    List<Map<String, Object>> resMapList =
        (List<Map<String, Object>>) res.getResult().get(JsonKey.RESPONSE);
    if (null != resMapList && !resMapList.isEmpty()) {
      for (Map<String, Object> map : resMapList) {
        String name = (String) map.get(JsonKey.NAME);
        if (null != name && "ORG_TYPE_0001".equalsIgnoreCase(name)) {
          orgTypeId1 = (String) map.get(JsonKey.ID);
          Assert.assertTrue(null != orgTypeId1);
        }
      }
    }
  }

  @Test
  public void test38OrgTypeList() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_ORG_TYPE_LIST.getValue());
    reqObj.setRequestId(ExecutionContext.getRequestId());
    reqObj.setEnv(1);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testCreateOrgTypeWithSameName() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG_TYPE.getValue());
    reqObj.setRequestId(ExecutionContext.getRequestId());
    reqObj.setEnv(1);
    reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_0001");
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateOrgType() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG_TYPE.getValue());
    reqObj.setRequestId(ExecutionContext.getRequestId());
    reqObj.setEnv(1);
    reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_203");
    reqObj.getRequest().put(JsonKey.ID, orgTypeId1);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    assertEquals("SUCCESS", response.getResult().get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateOrgTypeWithExistingName() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG_TYPE.getValue());
    reqObj.setRequestId(ExecutionContext.getRequestId());
    reqObj.setEnv(1);
    reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_0002");
    reqObj.getRequest().put(JsonKey.ID, orgTypeId1);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateOrgTypeWithWrongId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG_TYPE.getValue());
    reqObj.setRequestId(ExecutionContext.getRequestId());
    reqObj.setEnv(1);
    reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_12");
    String id = orgTypeId2 + "1";
    reqObj.getRequest().put(JsonKey.ID, id);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("200 second"), Response.class);
    assertEquals("SUCCESS", response.getResult().get(JsonKey.RESPONSE));
  }

  private Request createOrgRequest(
      boolean isRootOrg, boolean isChannelRequired, boolean isExternalIdRequired, String email) {
    Request request = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, USER_ID);
    request.setContext(innerMap);
    HashMap<String, Object> req = new HashMap<>();
    req.put(JsonKey.ORG_NAME, ORG_NAME);
    req.put(JsonKey.IS_ROOT_ORG, isRootOrg);
    if (isRootOrg || isChannelRequired) {
      req.put(JsonKey.CHANNEL, CHANNEL);
    }
    if (email != null) {
      req.put(JsonKey.EMAIL, email);
    }

    if (isExternalIdRequired) req.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    request.setRequest(req);
    request.setOperation(ActorOperations.CREATE_ORG.getValue());
    return request;
  }

  private Request UpdateOrgStatusRequest(String orgId, BigInteger status) {
    Request request = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, USER_ID);
    request.setContext(innerMap);
    HashMap<String, Object> req = new HashMap<>();
    req.put(JsonKey.ORG_ID, orgId);
    req.put(JsonKey.STATUS, status);
    request.setRequest(req);
    request.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());
    return request;
  }

  @SuppressWarnings("unchecked")
  private void mockCasandraGetRecordsByProperty(boolean isFailure) {
    if (isFailure)
      when(cassandraOperation.getRecordsByProperty(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
          .thenReturn(createGetRecordByIdFailureResponse());
    else
      when(cassandraOperation.getRecordsByProperty(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
          .thenReturn(createGetRecordByIdSuccessResponse());
  }

  @SuppressWarnings("unchecked")
  private void mockCasandraGetRecordById(boolean isFailure) {
    if (isFailure)
      when(cassandraOperation.getRecordById(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
          .thenReturn(createGetRecordByIdFailureResponse());
    else
      when(cassandraOperation.getRecordById(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
          .thenReturn(createGetRecordByIdSuccessResponse());
  }

  @SuppressWarnings("unchecked")
  private void mockCasandraGetRecordsByProperties(boolean isFailure) {
    if (isFailure)
      when(cassandraOperation.getRecordsByProperties(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
          .thenReturn(createGetRecordByIdFailureResponse());
    else
      when(cassandraOperation.getRecordsByProperties(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
          .thenReturn(createGetRecordByIdSuccessResponse());
  }

  @SuppressWarnings("unchecked")
  private void mockCasandraInsertResponse() {
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(createCassandraCommonSuccessResponse());
  }

  @SuppressWarnings("unchecked")
  private void mockCasandraUpdateRecordResponse() {
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(createCassandraCommonSuccessResponse());
  }

  @SuppressWarnings("unchecked")
  private Response createGetRecordByIdFailureResponse() {
    Response response = new Response();
    List<Map<String, Object>> result = new ArrayList<>();
    response.put(JsonKey.RESPONSE, result);
    return response;
  }

  private Response createGetRecordByIdSuccessResponse() {
    Response response = new Response();
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, ROOT_ORG_ID);
    orgMap.put(JsonKey.ROOT_ORG_ID, ROOT_ORG_ID);
    orgMap.put(JsonKey.CHANNEL, CHANNEL);
    orgMap.put(JsonKey.STATUS, ProjectUtil.OrgStatus.ACTIVE.getValue());
    List<Map<String, Object>> result = new ArrayList<>();
    result.add(orgMap);
    response.put(JsonKey.RESPONSE, result);
    return response;
  }

  private Response createCassandraCommonSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }
}
