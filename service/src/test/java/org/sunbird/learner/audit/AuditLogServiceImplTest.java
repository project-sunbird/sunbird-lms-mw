package org.sunbird.learner.audit;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EnrolmentType;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.audit.impl.ActorAuditLogServiceImpl;
import org.sunbird.learner.util.AuditOperation;
import org.sunbird.learner.util.Util;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore
public class AuditLogServiceImplTest {

  private static ActorSystem system;
  private static final Props props = Props.create(ActorAuditLogServiceImpl.class);
  private static List<String> auditIds = new ArrayList<>();

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  @Ignore
  public void testACreateUserAuditLog() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.PROCESS_AUDIT_LOG.getValue());
    Request request = new Request();
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_18182");
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_18182@gmail.com");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.PROVIDER, "BLR");
    innerMap.put(JsonKey.PHONE, "9874561230");
    innerMap.put(JsonKey.PHONE_VERIFIED, true);
    innerMap.put(JsonKey.EMAIL_VERIFIED, true);
    List<String> roleList = new ArrayList<>();
    roleList.add("CONTENT_CURATION");
    roleList.add("CONTENT_CREATION");
    roleList.add("MEMBERSHIP_MANAGEMENT");
    innerMap.put(JsonKey.ROLES, roleList);
    // Add Address
    List<Map<String, Object>> addrList = new ArrayList<Map<String, Object>>();
    Map<String, Object> address = new HashMap<String, Object>();
    address.put(JsonKey.ADDRESS_LINE1, "addr line1a");
    address.put(JsonKey.CITY, "city1");
    Map<String, Object> address2 = new HashMap<String, Object>();
    address2.put(JsonKey.ADDRESS_LINE1, "addr line1b");
    address2.put(JsonKey.CITY, "city2");
    addrList.add(address);
    addrList.add(address2);
    innerMap.put(JsonKey.ADDRESS, addrList);
    // Add Job Profile
    List<Map<String, Object>> jobProfileList = new ArrayList<Map<String, Object>>();
    Map<String, Object> jobProfile = new HashMap<String, Object>();
    jobProfile.put(JsonKey.JOB_NAME, "job title");
    jobProfile.put(JsonKey.ORG_NAME, "KA Org");
    jobProfileList.add(jobProfile);
    innerMap.put(JsonKey.JOB_PROFILE, jobProfileList);
    // Add Education
    List<Map<String, Object>> eduList = new ArrayList<Map<String, Object>>();
    Map<String, Object> education = new HashMap<String, Object>();
    education.put(JsonKey.DEGREE, "degree");
    education.put(JsonKey.NAME, "College Name");
    innerMap.put(JsonKey.USER_ID, "01234567891011");
    eduList.add(education);
    innerMap.put(JsonKey.EDUCATION, eduList);

    Map<String, Object> requestMap = new HashMap<String, Object>();
    requestMap.put(JsonKey.USER, innerMap);
    request.setOperation(ActorOperations.CREATE_USER.getValue());
    request.setRequest(requestMap);

    Response response = new Response();
    response.setResponseCode(ResponseCode.OK);
    response.put(JsonKey.USER_ID, "01234567891011");
    AuditOperation op =
        (AuditOperation) Util.auditLogUrlMap.get(ActorOperations.CREATE_USER.getValue());

    Map<String, Object> opMap = new HashMap<>();
    opMap.put(JsonKey.OPERATION, op);
    opMap.put(JsonKey.REQUEST, request);
    opMap.put(JsonKey.RESPONSE, response);
    reqObj.setRequest(opMap);
    subject.tell(reqObj, probe.getRef());
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }

    Request requestObj = new Request();
    requestObj.setOperation(ActorOperations.SEARCH_AUDIT_LOG.getValue());
    Map<String, Object> filters = new HashMap<>();
    Map<String, Object> filterMap = new HashMap<>();
    filters.put(JsonKey.OBJECT_ID, "01234567891011");
    filters.put(JsonKey.OPERATION_TYPE, JsonKey.CREATE);
    filters.put(JsonKey.OBJECT_TYPE, ProjectUtil.ObjectTypes.user.getValue());
    filterMap.put(JsonKey.FILTERS, filters);
    requestObj.setRequest(filterMap);
    subject.tell(requestObj, probe.getRef());
    Response responseObj = probe.expectMsgClass(duration("300 second"), Response.class);
    Assert.assertEquals(ResponseCode.OK, responseObj.getResponseCode());
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    List<Object> auditRecordList =
        (List<Object>)
            ((Map<String, Object>) responseObj.getResult().get(JsonKey.RESPONSE))
                .get(JsonKey.CONTENT);
    Map<String, Object> auditRecord = (Map<String, Object>) auditRecordList.get(0);
    auditIds.add((String) auditRecord.get(JsonKey.IDENTIFIER));
    Assert.assertEquals(
        1L,
        ((Map<String, Object>) responseObj.getResult().get(JsonKey.RESPONSE)).get(JsonKey.COUNT));
  }

  @SuppressWarnings({"unchecked"})
  @Ignore
  public void testCreateOrgAuditLog() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.PROCESS_AUDIT_LOG.getValue());
    Request request = new Request();
    Map<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "CBSE");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put("orgCode", "CBSE");
    orgMap.put("isRootOrg", true);
    orgMap.put("channel", "test");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    request.setOperation(ActorOperations.CREATE_ORG.getValue());
    request.setRequest(innerMap);

    Response response = new Response();
    response.setResponseCode(ResponseCode.OK);
    response.put(JsonKey.ORGANISATION_ID, "01234567891012");
    AuditOperation op =
        (AuditOperation) Util.auditLogUrlMap.get(ActorOperations.CREATE_ORG.getValue());

    Map<String, Object> opMap = new HashMap<>();
    opMap.put(JsonKey.OPERATION, op);
    opMap.put(JsonKey.REQUEST, request);
    opMap.put(JsonKey.RESPONSE, response);
    reqObj.setRequest(opMap);
    subject.tell(reqObj, probe.getRef());
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }

    Request requestObj = new Request();
    requestObj.setOperation(ActorOperations.SEARCH_AUDIT_LOG.getValue());
    Map<String, Object> filters = new HashMap<>();
    Map<String, Object> filterMap = new HashMap<>();
    filters.put(JsonKey.OBJECT_ID, "01234567891012");
    filters.put(JsonKey.OPERATION_TYPE, JsonKey.CREATE);
    filters.put(JsonKey.OBJECT_TYPE, ProjectUtil.ObjectTypes.organisation.getValue());
    filterMap.put(JsonKey.FILTERS, filters);
    requestObj.setRequest(filterMap);
    subject.tell(requestObj, probe.getRef());
    @SuppressWarnings("deprecation")
    Response responseObj = probe.expectMsgClass(duration("300 second"), Response.class);
    Assert.assertEquals(ResponseCode.OK, responseObj.getResponseCode());
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    List<Object> auditRecordList =
        (List<Object>)
            ((Map<String, Object>) responseObj.getResult().get(JsonKey.RESPONSE))
                .get(JsonKey.CONTENT);
    Map<String, Object> auditRecord = (Map<String, Object>) auditRecordList.get(0);
    auditIds.add((String) auditRecord.get(JsonKey.IDENTIFIER));
    Assert.assertEquals(
        1L,
        ((Map<String, Object>) responseObj.getResult().get(JsonKey.RESPONSE)).get(JsonKey.COUNT));
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  @Ignore
  public void testCreateBatchAuditLog() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.PROCESS_AUDIT_LOG.getValue());
    Request request = new Request();
    Map<String, Object> innerMap = new HashMap<>();
    Map<String, Object> batchMap = new HashMap<String, Object>();
    batchMap.put(JsonKey.COURSE_ID, "do_123");
    batchMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    batchMap.put(JsonKey.NAME, "BATCH 1");
    batchMap.put(JsonKey.ENROLLMENT_TYPE, EnrolmentType.inviteOnly.getVal());
    batchMap.put(JsonKey.START_DATE, "2017-10-10");
    batchMap.put(JsonKey.END_DATE, "2017-10-10");
    List<String> course = new ArrayList<>();
    course.add("ORG_001");
    batchMap.put(JsonKey.COURSE_CREATED_FOR, course);
    innerMap.put(JsonKey.BATCH, batchMap);
    request.setRequest(innerMap);
    request.setOperation(ActorOperations.CREATE_BATCH.getValue());

    Response response = new Response();
    response.setResponseCode(ResponseCode.OK);
    response.put(JsonKey.BATCH_ID, "01234567891013");
    AuditOperation op =
        (AuditOperation) Util.auditLogUrlMap.get(ActorOperations.CREATE_BATCH.getValue());

    Map<String, Object> opMap = new HashMap<>();
    opMap.put(JsonKey.OPERATION, op);
    opMap.put(JsonKey.REQUEST, request);
    opMap.put(JsonKey.RESPONSE, response);
    reqObj.setRequest(opMap);
    subject.tell(reqObj, probe.getRef());
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }

    Request requestObj = new Request();
    requestObj.setOperation(ActorOperations.SEARCH_AUDIT_LOG.getValue());
    Map<String, Object> filters = new HashMap<>();
    Map<String, Object> filterMap = new HashMap<>();
    filters.put(JsonKey.OBJECT_ID, "01234567891013");
    filters.put(JsonKey.OPERATION_TYPE, JsonKey.CREATE);
    filters.put(JsonKey.OBJECT_TYPE, ProjectUtil.ObjectTypes.batch.getValue());
    filterMap.put(JsonKey.FILTERS, filters);
    requestObj.setRequest(filterMap);
    subject.tell(requestObj, probe.getRef());
    Response responseObj = probe.expectMsgClass(duration("200 second"), Response.class);
    Assert.assertEquals(ResponseCode.OK, responseObj.getResponseCode());
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    List<Object> auditRecordList =
        (List<Object>)
            ((Map<String, Object>) responseObj.getResult().get(JsonKey.RESPONSE))
                .get(JsonKey.CONTENT);
    Map<String, Object> auditRecord = (Map<String, Object>) auditRecordList.get(0);
    auditIds.add((String) auditRecord.get(JsonKey.IDENTIFIER));
    Assert.assertEquals(
        1L,
        ((Map<String, Object>) responseObj.getResult().get(JsonKey.RESPONSE)).get(JsonKey.COUNT));
  }

  @AfterClass
  public static void delete() {
    for (String auditId : auditIds) {
      ElasticSearchUtil.removeData(
          EsIndex.sunbirdDataAudit.getIndexName(), EsType.history.getTypeName(), auditId);
    }
  }
}
