package org.sunbird.learner.actors.coursemanagement;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.text.SimpleDateFormat;
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
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.CourseBatchNotificationActor;
import org.sunbird.learner.util.EkStepRequestUtil;
import org.sunbird.models.course.batch.CourseBatch;

/*
 * @author github.com/iostream04
 *
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class})
@PowerMockIgnore("javax.management.*")
public class CourseBatchNotificationActorTest {

  private TestKit probe;
  private ActorRef subject;

  private static final String FIRST_NAME = "Test User";
  private static final String USER_ID = "testUserId";
  private static final String USER_ID_OLD = "testUserIdOld";
  private static final String USER_ID_NEW = "testUserIdNew";
  private List<String> recipentEmails;
  private List<String> recipentUserIds;
  private static final String emailId = "user@test.com";
  private static final String orgName = "testOrg";
  private static final String TEMPLATE = "template";

  private static ActorSystem system;
  private static final Props props = Props.create(CourseBatchNotificationActor.class);
  private static CassandraOperation cassandraOperation;

  SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    PowerMockito.mockStatic(EkStepRequestUtil.class);
  }

  @Before
  public void beforeEachTest() {
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    Mockito.reset(cassandraOperation);
  }

  @Test // Needs to test run
  public void testCourseBatchEnrollForLearnerFailure() {
    mockCassandraRequestForReadRecordById();
    mockCassandraRequestForReadTemplate();
    Response response = getEnrollFailureEmailNotificationForLearnerTestResponse();
    Assert.assertTrue(null != response && response.getResponseCode() != ResponseCode.OK);
  }

  @Test
  public void testCourseBatchEnrollForLearnerSucess() {
    mockCassandraRequestForReadRecordById();
    mockCassandraRequestForReadTemplate();
    Response response = getEnrollSucessEmailNotificationForLearnerTestResponse();
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testCourseBatchEnrollForMentorFailure() {
    mockCassandraRequestForReadRecordById();
    Response response = getEnrollFailureEmailNotificationForMentorTestResponse();
    Assert.assertTrue(null != response && response.getResponseCode() != ResponseCode.OK);
  }

  @Test
  public void testCourseBatchEnrollForMentorSucess() {
    mockCassandraRequestForReadRecordById();
    mockCassandraRequestForReadTemplate();
    Response response = getEnrollSucessEmailNotificationForMentorTestResponse();
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testCourseBatchUpdateSucess() {
    mockCassandraRequestForReadRecordById();
    mockCassandraRequestForReadTemplate();
    Response response = getUpdateSucessEmailNotificationTestResponse();
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testCourseBatchUpdateFailure() {
    mockCassandraRequestForReadRecordById();
    mockCassandraRequestForReadTemplate();
    Response response = getUpdateFailureEmailNotificationTestResponse();
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testCourseBatchBulkAddSucess() {
    mockCassandraRequestForReadRecordById();
    mockCassandraRequestForReadTemplate();
    Response response = getBulkSucessEmailNotificationTestResponse(JsonKey.ADD);
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testCourseBatchBulkRemoveSucess() {
    mockCassandraRequestForReadRecordById();
    mockCassandraRequestForReadTemplate();
    Response response = getBulkSucessEmailNotificationTestResponse(JsonKey.REMOVE);
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testCourseBatchBulkAddFailure() {
    mockCassandraRequestForReadRecordById();
    mockCassandraRequestForReadTemplate();
    Response response = getBulkFailureEmailNotificationTestResponse(JsonKey.ADD);
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testCourseBatchBulkRemoveFailure() {
    mockCassandraRequestForReadRecordById();
    mockCassandraRequestForReadTemplate();
    Response response = getBulkFailureEmailNotificationTestResponse(JsonKey.REMOVE);
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  private Response getBulkFailureEmailNotificationTestResponse(String operationType) {
    Request request =
        createRequestObjectForBulkOperation(
            createCourseBatchObject(false, JsonKey.SUNBIRD), operationType);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    return response;
  }

  private Response getBulkSucessEmailNotificationTestResponse(String operationType) {

    Request request =
        createRequestObjectForBulkOperation(
            createCourseBatchObject(true, JsonKey.SUNBIRD), operationType);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    return response;
  }

  private Response getUpdateFailureEmailNotificationTestResponse() {
    Request request =
        createRequestObjectForUpdateOperation(
            createCourseBatchObject(false, JsonKey.OLD),
            createCourseBatchObject(false, JsonKey.NEW));
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    return response;
  }

  private Response getUpdateSucessEmailNotificationTestResponse() {
    Request request =
        createRequestObjectForUpdateOperation(
            createCourseBatchObject(true, JsonKey.OLD), createCourseBatchObject(true, JsonKey.NEW));
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    return response;
  }

  private Response getEnrollFailureEmailNotificationForMentorTestResponse() {
    Request request =
        createRequestObjectForEnrollOperation(
            createCourseBatchObject(false), createCourseMap(), JsonKey.BATCH_MENTOR_UNENROL);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    return response;
  }

  private Response getEnrollSucessEmailNotificationForMentorTestResponse() {
    Request request =
        createRequestObjectForEnrollOperation(
            createCourseBatchObject(true), createCourseMap(), JsonKey.BATCH_MENTOR_ENROL);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    return response;
  }

  private Response getEnrollSucessEmailNotificationForLearnerTestResponse() {
    Request request =
        createRequestObjectForEnrollOperation(
            createCourseBatchObject(true), createCourseMap(), JsonKey.BATCH_LEARNER_ENROL);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    return response;
  }

  private Response getEnrollFailureEmailNotificationForLearnerTestResponse() {
    Request request =
        createRequestObjectForEnrollOperation(
            createCourseBatchObject(false), createCourseMap(), JsonKey.BATCH_LEARNER_ENROL);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    return response;
  }

  private Request createRequestObjectForEnrollOperation(
      CourseBatch courseBatch, Map<String, Object> courseMap, String operationType) {
    Request request = new Request();
    Map<String, Object> requestMap = new HashMap<>();
    request.setOperation(ActorOperations.BATCH_OPERATION.getValue());
    requestMap.put(JsonKey.COURSE_BATCH, courseBatch);
    requestMap.put(JsonKey.COURSE_MAP, courseMap);
    requestMap.put(JsonKey.OPERATION_TYPE, operationType);
    request.setRequest(requestMap);
    return request;
  }

  private Request createRequestObjectForUpdateOperation(
      CourseBatch CourseBatchOld, CourseBatch courseBatchNew) {
    Request request = new Request();
    Map<String, Object> requestMap = new HashMap<>();
    request.setOperation(ActorOperations.BATCH_UPDATE.getValue());
    requestMap.put(JsonKey.OLD, CourseBatchOld);
    requestMap.put(JsonKey.NEW, courseBatchNew);
    request.setRequest(requestMap);
    return request;
  }

  private Request createRequestObjectForBulkOperation(
      CourseBatch courseBatch, String OperationType) {
    Request request = new Request();
    Map<String, Object> requestMap = new HashMap<>();
    request.setOperation(ActorOperations.BATCH_BULK.getValue());
    requestMap.put(JsonKey.COURSE_BATCH, courseBatch);
    requestMap.put(JsonKey.OPERATION_TYPE, OperationType);
    request.setRequest(requestMap);
    return request;
  }

  private Map<String, Object> createCourseMap() {
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.USER_ID, USER_ID);
    return map;
  }

  private CourseBatch createCourseBatchObject(boolean testType) {
    CourseBatch courseBatch = new CourseBatch();
    List<String> mentors = new ArrayList<>();
    Map<String, Boolean> participants = new HashMap<>();
    if (testType) {
      mentors.add(USER_ID);
      participants.put(USER_ID, true);
    }
    courseBatch.setMentors(mentors);
    courseBatch.setParticipant(participants);
    Map<String, String> courseAdditionalInfo = new HashMap<>();
    courseAdditionalInfo.put(JsonKey.ORG_NAME, orgName);
    courseBatch.setCourseAdditionalInfo(courseAdditionalInfo);

    return courseBatch;
  }

  private CourseBatch createCourseBatchObject(boolean testType, String type) {
    CourseBatch courseBatch = new CourseBatch();
    List<String> mentors = new ArrayList<>();
    Map<String, Boolean> participants = new HashMap<>();
    if (testType) {
      mentors.add(USER_ID);
      participants.put(USER_ID, true);
      if (type.equals(JsonKey.OLD)) {
        mentors.add(USER_ID_OLD);
        participants.put(USER_ID_OLD, true);
      }
      if (type.equals(JsonKey.NEW)) {
        mentors.add(USER_ID_NEW);
        participants.put(USER_ID_NEW, true);
      }
    }
    courseBatch.setMentors(mentors);
    courseBatch.setParticipant(participants);
    Map<String, String> courseAdditionalInfo = new HashMap<>();
    courseAdditionalInfo.put(JsonKey.ORG_NAME, orgName);
    courseBatch.setCourseAdditionalInfo(courseAdditionalInfo);
    return courseBatch;
  }

  private Response StringTemplateResponse() {
    Response response = new Response();
    List<Map<String, Object>> result = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put(TEMPLATE, new String());
    result.add(map);
    response.put(JsonKey.RESPONSE, result);
    return response;
  }

  private Response createUser() {
    Response response = new Response();
    Map<String, Object> userMap = new HashMap<>();
    userMap.put(JsonKey.FIRST_NAME, FIRST_NAME);
    userMap.put(JsonKey.EMAIL, emailId);
    List<Map<String, Object>> result = new ArrayList<>();
    result.add(userMap);
    response.put(JsonKey.RESPONSE, result);
    return response;
  }

  @SuppressWarnings("unchecked")
  private void mockCassandraRequestForReadRecordById() {
    when(cassandraOperation.getRecordsByIdsWithSpecifiedColumns(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyList(), Mockito.anyList()))
        .thenReturn(createUser());
  }

  private void mockCassandraRequestForReadTemplate() {
    when(cassandraOperation.getRecordsByPrimaryKeys(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyList(), Mockito.anyString()))
        .thenReturn(StringTemplateResponse());
  }
}
