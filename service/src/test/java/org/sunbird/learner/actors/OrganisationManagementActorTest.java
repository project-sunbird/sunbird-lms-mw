package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.math.BigInteger;
import java.util.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ElasticSearchUtil.class,
  ServiceFactory.class,
  DataCacheHandler.class,
  RequestRouter.class,
  ElasticSearchUtil.class
})
@PowerMockIgnore({"javax.management.*"})
public class OrganisationManagementActorTest {

  private static final ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(OrganisationManagementActor.class);
  //    private static Response getAllRecordValue = getCassandraSuccessResponse();
  private static CassandraOperationImpl cassandraOperation;

  @Before
  public void beforeTest() {

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);

    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getCassandraSuccessResponse());
    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getCassandraSuccessResponse());
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getCassandraSuccessResponse());
    when(cassandraOperation.getRecordsByProperty(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getCassandraSuccessResponse());

    PowerMockito.mockStatic(DataCacheHandler.class);
    when(DataCacheHandler.getOrgTypeMap()).thenReturn(getOrgTypeMapResponse());

    PowerMockito.mockStatic(ElasticSearchUtil.class);
    when(ElasticSearchUtil.getDataByIdentifier(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getEsDataByIdentifierResponse());

    when(ElasticSearchUtil.complexSearch(
            Mockito.any(SearchDTO.class),
            Mockito.eq(ProjectUtil.EsIndex.sunbird.getIndexName()),
            Mockito.anyVararg()))
        .thenReturn(getComplexSearchResponse());
  }

  private Map<String, Object> getComplexSearchResponse() {

    HashMap<String, Object> response = new HashMap<>();
    List<Map<String, Object>> content = new ArrayList<>();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID, "anyId");
    innerMap.put(JsonKey.HASHTAGID, "anyHashTagId");
    content.add(innerMap);
    response.put(JsonKey.CONTENT, content);
    return response;
  }

  private static Map<String, Object> getEsDataByIdentifierResponse() {
    HashMap<String, Object> map = new HashMap<>();
    map.put(JsonKey.IS_ROOT_ORG, true);
    map.put(JsonKey.ID, "rootOrgId");
    map.put(JsonKey.CHANNEL, "anyChannel");
    return map;
  }

  private static Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  private static Response getCassandraSuccessResponse() {

    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, "ORGANISATION_ID");
    orgMap.put(JsonKey.STATUS, 1);
    orgMap.put(JsonKey.IS_ROOT_ORG, false);
    orgMap.put(JsonKey.IS_DELETED, false);
    list.add(orgMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private static Map<String, String> getOrgTypeMapResponse() {
    Map<String, String> orgMap = new HashMap<>();
    orgMap.put("orgtype", "orgTypeId");
    return orgMap;
  }

  @Test
  public void testCreateOrgSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CALLER_ID, "calledId");

    Map<String, Object> request = new HashMap();
    request.put(JsonKey.LOCATION_CODES, Arrays.asList("invalidLocationCode"));
    request.put(JsonKey.ORG_TYPE, "orgType");
    request.put(JsonKey.LOC_ID, "locationId");
    request.put(JsonKey.ADDRESS, new HashMap<>().put(JsonKey.CITY, "anyCity"));
    request.put(JsonKey.RELATION, "relation");
    request.put(JsonKey.PARENT_ORG_ID, "parentOrgId");
    request.put(JsonKey.REQUESTED_BY, "requestedBy");
    request.put(JsonKey.CHANNEL, "anyChannel");
    request.put(JsonKey.EXTERNAL_ID, null);
    request.put(JsonKey.ORGANISATION_ID, "orgId");

    reqObj.setRequest(request);
    reqObj.setContext(innerMap);
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null && "SUCCESS".equals(res.getResult().get(JsonKey.RESPONSE)));
  }

  @Test
  public void testUpdateOrgSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CALLER_ID, "calledId");

    Map<String, Object> request = new HashMap();
    request.put(JsonKey.LOCATION_CODES, Arrays.asList("invalidLocationCode"));
    request.put(JsonKey.ORG_TYPE, "orgType");
    request.put(JsonKey.LOC_ID, "locationId");
    request.put(JsonKey.ADDRESS, new HashMap<>().put(JsonKey.CITY, "anyCity"));
    request.put(JsonKey.RELATION, "relation");
    //        request.put(JsonKey.PARENT_ORG_ID, "parentOrgId");
    request.put(JsonKey.REQUESTED_BY, "requestedBy");
    request.put(JsonKey.CHANNEL, "anyChannel");
    request.put(JsonKey.EXTERNAL_ID, null);
    request.put(JsonKey.ORGANISATION_ID, "orgId");

    reqObj.setRequest(request);
    reqObj.setContext(innerMap);
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null && "SUCCESS".equals(res.getResult().get(JsonKey.RESPONSE)));
  }

  @Test
  public void testupdateOrgStatusSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CALLER_ID, "calledId");

    Map<String, Object> request = new HashMap();

    request.put(JsonKey.REQUESTED_BY, "requestedBy");
    request.put(JsonKey.ORGANISATION_ID, "orgId");
    request.put(JsonKey.STATUS, new BigInteger("1"));

    reqObj.setRequest(request);
    reqObj.setContext(innerMap);
    reqObj.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null && "SUCCESS".equals(res.getResult().get(JsonKey.RESPONSE)));
  }

  @Test
  public void testGetOrgDetailsSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CALLER_ID, "calledId");

    Map<String, Object> request = new HashMap();

    request.put(JsonKey.USER_ID, "anyUserId");
    request.put(JsonKey.ORGANISATION_ID, "orgId");
    request.put(JsonKey.REQUESTED_BY, "reqBy");
    request.put(JsonKey.ROLES, Arrays.asList("anyRole"));
    request.put(JsonKey.STATUS, new BigInteger("1"));

    reqObj.setRequest(request);
    reqObj.setContext(innerMap);
    reqObj.setOperation(ActorOperations.GET_ORG_DETAILS.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null);
  }

  @Test
  public void testAddMemberOrganisationSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CALLER_ID, "calledId");

    Map<String, Object> request = new HashMap();

    request.put(JsonKey.USER_ID, "anyUserId");
    request.put(JsonKey.ORGANISATION_ID, "orgId");
    request.put(JsonKey.REQUESTED_BY, "reqBy");
    request.put(JsonKey.ROLES, Arrays.asList("anyRole"));
    request.put(JsonKey.STATUS, new BigInteger("1"));

    reqObj.setRequest(request);
    reqObj.setContext(innerMap);
    reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null);
  }

  @Test
  public void testRemoveMemberOrganisationSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CALLER_ID, "calledId");

    Map<String, Object> request = new HashMap();

    request.put(JsonKey.USER_ID, "anyUserId");
    request.put(JsonKey.ORGANISATION_ID, "orgId");
    request.put(JsonKey.REQUESTED_BY, "reqBy");
    request.put(JsonKey.ROLES, Arrays.asList("anyRole"));
    request.put(JsonKey.STATUS, new BigInteger("1"));

    reqObj.setRequest(request);
    reqObj.setContext(innerMap);
    reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null);
  }

  @Test
  public void testGetOrgTypeListSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CALLER_ID, "calledId");

    Map<String, Object> request = new HashMap();

    request.put(JsonKey.USER_ID, "anyUserId");
    request.put(JsonKey.ORGANISATION_ID, "orgId");
    request.put(JsonKey.REQUESTED_BY, "reqBy");
    request.put(JsonKey.ROLES, Arrays.asList("anyRole"));
    request.put(JsonKey.STATUS, new BigInteger("1"));

    reqObj.setRequest(request);
    reqObj.setContext(innerMap);
    reqObj.setOperation(ActorOperations.GET_ORG_TYPE_LIST.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null);
  }

  @Test
  public void testCreateOrgTypeSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getCassandraSuccessResponseOne());

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CALLER_ID, "calledId");

    Map<String, Object> request = new HashMap();

    request.put(JsonKey.USER_ID, "anyUserId");
    request.put(JsonKey.ORGANISATION_ID, "orgId");
    request.put(JsonKey.REQUESTED_BY, "reqBy");
    request.put(JsonKey.ROLES, Arrays.asList("anyRole"));
    request.put(JsonKey.STATUS, new BigInteger("1"));

    reqObj.setRequest(request);
    reqObj.setContext(innerMap);
    reqObj.setOperation(ActorOperations.CREATE_ORG_TYPE.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null);
  }

  private Response getCassandraSuccessResponseOne() {

    Response response = new Response();
    response.put(JsonKey.RESPONSE, Arrays.asList());
    return response;
  }

  @Test
  public void testUpdateOrgTypeSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(cassandraOperation.getRecordsByProperty(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getCassandraSuccessResponseOne());

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CALLER_ID, "calledId");

    Map<String, Object> request = new HashMap();
    request.put(JsonKey.USER_ID, "anyUserId");
    request.put(JsonKey.ORGANISATION_ID, "orgId");
    request.put(JsonKey.REQUESTED_BY, "reqBy");
    request.put(JsonKey.ROLES, Arrays.asList("anyRole"));
    request.put(JsonKey.STATUS, new BigInteger("1"));

    reqObj.setRequest(request);
    reqObj.setContext(innerMap);
    reqObj.setOperation(ActorOperations.UPDATE_ORG_TYPE.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null);
  }

  /*@Test
  public void test11createOrgForId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put(JsonKey.ORG_CODE, "CBSE");
    orgMap.put(JsonKey.HASHTAGID, HASH_TAG_ID);
    orgMap.put(JsonKey.PARENT_ORG_ID, parentOrgId);
    List<Map<String, Object>> contactDetails = new ArrayList<>();
    Map<String, Object> contactDetail = new HashMap<>();
    contactDetail.put("fax", "100");
    contactDetails.add(contactDetail);
    orgMap.put(JsonKey.CONTACT_DETAILS, contactDetails);
    orgMap.put(JsonKey.LOC_ID, LOCATION_ID);
    // orgMap.put(JsonKey.CHANNEL, "test");
    Map<String, Object> address = new HashMap<String, Object>();
    address.put(JsonKey.CITY, "Hyderabad");
    address.put("state", "Andra Pradesh");
    address.put("country", "India");
    address.put("zipCode", "466899");
    innerMap.put(JsonKey.ADDRESS, address);
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, "123234345");
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response resp = probe.expectMsgClass(duration("200 second"), Response.class);
    orgId = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
    System.out.println("orgId : " + orgId);
    Assert.assertTrue(null != orgId);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }

  @Test
  public void test11createOrgForIdWithDuplicateHashTagId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put(JsonKey.ORG_CODE, "CBSE");
    // orgMap.put(JsonKey.PROVIDER, PROVIDER);
    // orgMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    orgMap.put(JsonKey.HASHTAGID, HASH_TAG_ID);
    orgMap.put(JsonKey.PARENT_ORG_ID, parentOrgId);
    List<Map<String, Object>> contactDetails = new ArrayList<>();
    Map<String, Object> contactDetail = new HashMap<>();
    contactDetail.put("fax", "100");
    contactDetails.add(contactDetail);
    orgMap.put(JsonKey.CONTACT_DETAILS, contactDetails);
    orgMap.put(JsonKey.LOC_ID, LOCATION_ID);
    // orgMap.put(JsonKey.CHANNEL, "test");
    Map<String, Object> address = new HashMap<String, Object>();
    address.put(JsonKey.CITY, "Hyderabad");
    address.put("state", "Andra Pradesh");
    address.put("country", "India");
    address.put("zipCode", "466899");
    innerMap.put(JsonKey.ADDRESS, address);
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testInvalidOperation() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation("INVALID_OPERATION");

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
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
  public void test12testCreateOrgWithoutSourceAndExternalIdSuc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put("orgCode", "CBSE");
    orgMap.put("isRootOrg", false);
    orgMap.put("channel", CHANNEL);
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response resp = probe.expectMsgClass(duration("200 second"), Response.class);
    OrgIDWithoutSourceAndExternalId = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
    System.out.println("OrgIDWithoutSourceAndExternalId : " + OrgIDWithoutSourceAndExternalId);
    Assert.assertNotNull(OrgIDWithoutSourceAndExternalId);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }

  @Test
  public void test12testCreateOrgWithoutSourceAndExternalIdSucDuplicateChannel() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put("orgCode", "CBSE");
    orgMap.put("isRootOrg", true);
    orgMap.put("channel", CHANNEL);
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void test13CreateOrgWithSourceAndExternalIdSuc() {

    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put("orgCode", "CBSE");
    orgMap.put(JsonKey.PROVIDER, PROVIDER);
    orgMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    // orgMap.put("channel", "test1");
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response resp = probe.expectMsgClass(duration("200 second"), Response.class);
    OrgIdWithSourceAndExternalId = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
    Assert.assertNotNull(OrgIdWithSourceAndExternalId);
  }

  @Test
  public void test13CreateOrgWithSourceAndExternalIdSucDuplicate() {

    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put("orgCode", "CBSE");
    orgMap.put(JsonKey.PROVIDER, PROVIDER);
    orgMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    // orgMap.put("channel", "test1");
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void test14CreateOrgWithSameSourceAndExternalIdExc() {
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put("orgCode", "CBSE");
    orgMap.put(JsonKey.PROVIDER, PROVIDER);
    orgMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    // orgMap.put("channel", CHANNEL);
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void test15CreateOrgWithBlankSourceAndExternalIdExc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put("orgCode", "CBSE");
    orgMap.put(JsonKey.PROVIDER, null);
    orgMap.put("externalId", null);
    // orgMap.put("channel", CHANNEL);
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  // @Test
  public void test16CreateOrgRootWithoutChannelExc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "AP Board");
    orgMap.put(JsonKey.DESCRIPTION, "AndhraPradesh Board");
    orgMap.put(JsonKey.ORG_TYPE, "Training");
    orgMap.put(JsonKey.CHANNEL, null);
    orgMap.put("preferredLanguage", "English");
    orgMap.put("homeUrl", "https:testUrl");
    orgMap.put(JsonKey.ORG_CODE, "AP");
    orgMap.put(JsonKey.IS_ROOT_ORG, true);
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void test17CreateOrgInvalidParentIdExc() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "Tamil Nadu ");
    orgMap.put(JsonKey.DESCRIPTION, "Tamil Nadu Board");
    orgMap.put(JsonKey.PARENT_ORG_ID, "CBSE");
    orgMap.put(JsonKey.ORG_TYPE, "Training");
    orgMap.put("imgUrl", "https://testimgUrl");
    orgMap.put(JsonKey.CHANNEL, "Ekstep");
    orgMap.put("preferredLanguage", "Tamil");
    orgMap.put("homeUrl", "https:testUrl");
    orgMap.put(JsonKey.ORG_CODE, "TN");
    orgMap.put(JsonKey.IS_ROOT_ORG, false);
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void test20UpdateStatusSuc() {
    TestKit probe = new TestKit(system);

    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, orgId);
    orgMap.put(JsonKey.STATUS, new BigInteger(String.valueOf(OrgStatus.RETIRED.getValue())));
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void test20UpdateStatusSucInvalidOrgId() {
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
  public void test20UpdateStatusSucInvalidStateTransition() {
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
  public void test21UpdateStatusEx() {
    TestKit probe = new TestKit(system);

    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.PROVIDER, source);
    orgMap.put(JsonKey.EXTERNAL_ID, externalId);
    orgMap.put(JsonKey.STATUS, new BigInteger("10"));
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void test22UpdateOrgExc() {
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
  public void test22UpdateOrgExcInvalidOrgId() {
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
  public void test23UpdateOrgSuc() {
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
  public void test23UpdateOrgExc() {
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
  public void test23UpdateOrgSuc001() {
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
    */
  /*
   * Map<String ,Object> address = new HashMap<>(); address.put(JsonKey.CITY ,
   * "STATE"); //orgMap.put(JsonKey.ADDRESS , address);
   */
  /*
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void test23UpdateOrgSuc002() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_ID, OrgIdWithSourceAndExternalId);
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
  public void test24GetOrgSuc() {
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
  public void test25GetOrgExc() {
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
  public void test26AddMemberToOrgExc() {
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
  public void test26AddMemberToOrgExc2() {
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
  public void test27AddMemberToOrgSuc() {
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
  public void test27AddMemberToOrgSuc001() {
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
  public void test27AddMemberToOrgSuc001AddAgain() {
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
  public void test27AddMemberToOrgSuc001AddAgain2() {
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
  public void test27AddMemberToOrgSuc001Exc() {
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
  public void test27AddMemberToOrgExpUserIdNull() {
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
  public void test27AddMemberToOrgExpInvalidOrgId() {
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
  public void test27AddMemberToOrgExpInvalidUserId() {
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
  public void test28RemoveMemberFromOrgSuc001() {
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
  public void test28RemoveMemberFromOrgExpNullOrgId() {
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
  public void test28RemoveMemberFromOrgExpNullUsrId() {
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
  public void test28RemoveMemberFromOrgExpInvalidRequestData() {
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
  public void test29RemoveMemberFromOrgExc() {
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
  public void test30JoinMemberOrgSuc() {
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
  public void test31JoinMemberOrgExc() {
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
  public void test32ApproveMemberOrgSuc() {
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
  public void test33ApproveMemberFromOrgExc() {
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
  public void test34RejectMemberOrgSuc() {
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
  public void test35RejectMemberOrgExc() {
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
  public void test36CreateOrgType() {
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
  public void test37CreateOrgType() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG_TYPE.getValue());
    reqObj.setRequestId(ExecutionContext.getRequestId());
    reqObj.setEnv(1);
    reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_0002");
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
        if (null != name && "ORG_TYPE_0002".equalsIgnoreCase(name)) {
          orgTypeId2 = (String) map.get(JsonKey.ID);
          Assert.assertTrue(null != orgTypeId2);
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
  public void test39CreateOrgTypeWithSameName() {
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
  public void test40UpdateOrgType() {
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
  public void test41UpdateOrgTypeWithExistingName() {
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
  public void test42UpdateOrgTypeWithWrongId() {
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

  @AfterClass
  public static void delete() {
    try {
      CassandraOperation operation = ServiceFactory.getInstance();
      operation.deleteRecord(orgTypeDbInfo.getKeySpace(), orgTypeDbInfo.getTableName(), orgTypeId1);
      operation.deleteRecord(orgTypeDbInfo.getKeySpace(), orgTypeDbInfo.getTableName(), orgTypeId2);
      // operation.deleteRecord(userManagementDB.getKeySpace(),
      // userManagementDB.getTableName(),
      // usrId);
      operation.deleteRecord(addressDB.getKeySpace(), addressDB.getTableName(), addressId);
      operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), orgId);
      operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), HASH_TAG_ID);
      operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), parentOrgId);
      operation.deleteRecord(locationDB.getKeySpace(), locationDB.getTableName(), LOCATION_ID);
      System.out.println("1 " + orgId);

      operation.deleteRecord(
          orgDB.getKeySpace(), orgDB.getTableName(), OrgIDWithoutSourceAndExternalId);
      System.out.println("2 " + OrgIDWithoutSourceAndExternalId);

      operation.deleteRecord(
          orgDB.getKeySpace(), orgDB.getTableName(), OrgIdWithSourceAndExternalId);
      System.out.println("3 " + OrgIdWithSourceAndExternalId);

    } catch (Exception th) {
      ProjectLogger.log(th.getMessage(), th);
    }
    if (!StringUtils.isBlank(usrId)) {
      ElasticSearchUtil.removeData(
          ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), usrId);
    }

    if (!StringUtils.isBlank(USER_ID)) {
      ElasticSearchUtil.removeData(
          ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.user.getTypeName(),
          USER_ID);
    }

    if (!StringUtils.isBlank(USER_ID + "01")) {
      ElasticSearchUtil.removeData(
          ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.user.getTypeName(),
          USER_ID + "01");
    }

    if (!StringUtils.isBlank(orgId)) {
      ElasticSearchUtil.removeData(
          ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.organisation.getTypeName(),
          orgId);
    }
    if (!StringUtils.isBlank(OrgIDWithoutSourceAndExternalId)) {
      ElasticSearchUtil.removeData(
          ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.organisation.getTypeName(),
          OrgIDWithoutSourceAndExternalId);
    }
    if (!StringUtils.isBlank(OrgIdWithSourceAndExternalId)) {
      ElasticSearchUtil.removeData(
          ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.organisation.getTypeName(),
          OrgIdWithSourceAndExternalId);
    }

    Map<String, Object> dbMap = new HashMap<>();
    dbMap.put(JsonKey.PROVIDER, PROVIDER);
    dbMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    Response result =
        operation.getRecordsByProperties(orgDB.getKeySpace(), orgDB.getTableName(), dbMap);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      for (Map<String, Object> res : list) {
        String id = (String) res.get(JsonKey.ID);
        System.out.println("ID is " + id);
        operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), id);
        ElasticSearchUtil.removeData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.organisation.getTypeName(),
            id);
      }
    }
    try {
      dbMap = new HashMap<>();
      dbMap.put(JsonKey.HASHTAGID, HASH_TAG_ID);
      // dbMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
      result = operation.getRecordsByProperties(orgDB.getKeySpace(), orgDB.getTableName(), dbMap);
      list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {
        for (Map<String, Object> res : list) {
          String id = (String) res.get(JsonKey.ID);
          System.out.println("ID is " + id);
          operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), id);
          ElasticSearchUtil.removeData(
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.organisation.getTypeName(),
              id);
        }
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    dbMap = new HashMap<>();
    dbMap.put(JsonKey.CHANNEL, CHANNEL);
    // dbMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    result = operation.getRecordsByProperties(orgDB.getKeySpace(), orgDB.getTableName(), dbMap);
    list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      for (Map<String, Object> res : list) {
        String id = (String) res.get(JsonKey.ID);
        System.out.println("ID is " + id);
        operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), id);
        ElasticSearchUtil.removeData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.organisation.getTypeName(),
            id);
      }
    }

    dbMap = new HashMap<>();
    dbMap.put(JsonKey.ORGANISATION_ID, orgId);
    // dbMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    result =
        operation.getRecordsByProperties(
            userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), dbMap);
    list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      for (Map<String, Object> res : list) {
        String id = (String) res.get(JsonKey.ID);
        System.out.println("ID is " + id);
        operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), id);
        ElasticSearchUtil.removeData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.organisation.getTypeName(),
            id);
      }
    }

    dbMap = new HashMap<>();
    dbMap.put(JsonKey.ORGANISATION_ID, orgId);
    // dbMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    result =
        operation.getRecordsByProperties(
            userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), dbMap);
    list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      for (Map<String, Object> res : list) {
        String id = (String) res.get(JsonKey.ID);
        System.out.println("ID is " + id);
        operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), id);
        ElasticSearchUtil.removeData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.organisation.getTypeName(),
            id);
      }
    }

    dbMap = new HashMap<>();
    dbMap.put(JsonKey.USER_ID, USER_ID);
    // dbMap.put(JsonKey.EXTERNAL_ID, EXTERNAL_ID);
    result =
        operation.getRecordsByProperties(
            userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), dbMap);
    list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      for (Map<String, Object> res : list) {
        String id = (String) res.get(JsonKey.ID);
        System.out.println("ID is " + id);
        operation.deleteRecord(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), id);
        */
  /*
   * ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
   * ProjectUtil.EsType.organisation.getTypeName(), id);
   */
  /*
      }
    }
  }*/
}
