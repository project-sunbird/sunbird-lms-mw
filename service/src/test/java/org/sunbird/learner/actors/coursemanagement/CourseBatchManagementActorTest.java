package org.sunbird.learner.actors.coursemanagement;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
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
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.CourseBatchManagementActor;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class})
@PowerMockIgnore("javax.management.*")
public class CourseBatchManagementActorTest {

  private static ActorSystem system;
  private static final Props props = Props.create(CourseBatchManagementActor.class);
  private static CassandraOperationImpl cassandraOperation;
  private static final String BATCH_ID = "0125706199365795842";
  private static final String NAME = "kirtiTest01";

  @BeforeClass
  public static void setUp() {
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    system = ActorSystem.create("system");
  }

  @Before
  public void beforeEachTest() {
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
  }

  @Test
  public void updateCoursebatchHavingOnlyStartDate() {
    String courseStatus = "alreadyStarted";
    Response response = getCassandraRecordByBatchId(courseStatus);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_COURSE.getValue());
    reqObj.getRequest().put(JsonKey.BATCH_ID, BATCH_ID);
    reqObj.getRequest().put(JsonKey.NAME, NAME);
    reqObj.getRequest().put(JsonKey.START_DATE, "2018-08-17");

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    List<Map<String, Object>> list = (List<Map<String, Object>>) res.get(JsonKey.RESPONSE);
    Assert.assertTrue(null == res);
  }

  @Test
  public void updateCoursebatchHavingOnlyEndDate() {
    String courseStatus = "alreadyStarted";
    Response response = getCassandraRecordByBatchId(courseStatus);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_COURSE.getValue());
    reqObj.getRequest().put(JsonKey.BATCH_ID, BATCH_ID);
    reqObj.getRequest().put(JsonKey.NAME, NAME);
    reqObj.getRequest().put(JsonKey.END_DATE, "2018-08-27");

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void updateCoursebatchHavingStartAndEndDate() {
    String courseStatus = "alreadyStarted";
    Response response = getCassandraRecordByBatchId(courseStatus);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_COURSE.getValue());
    reqObj.getRequest().put(JsonKey.BATCH_ID, BATCH_ID);
    reqObj.getRequest().put(JsonKey.NAME, NAME);
    reqObj.getRequest().put(JsonKey.START_DATE, "2018-08-17");
    reqObj.getRequest().put(JsonKey.END_DATE, "2018-08-27");

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  // course has not started yet
  @Test
  public void updateCoursebatchHavingStartDateAndEndDateAndCourseNotStarted() {

    String courseStatus = "notStarted";
    Response response = getCassandraRecordByBatchId(courseStatus);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_COURSE.getValue());
    reqObj.getRequest().put(JsonKey.BATCH_ID, BATCH_ID);
    reqObj.getRequest().put(JsonKey.NAME, NAME);
    reqObj.getRequest().put(JsonKey.START_DATE, "2018-08-17");
    reqObj.getRequest().put(JsonKey.END_DATE, "2018-08-10");

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void updateCoursebatchHavingOnlyStartDateAndCourseNotStarted() {

    String courseStatus = "notStarted";
    Response response = getCassandraRecordByBatchId(courseStatus);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_COURSE.getValue());
    reqObj.getRequest().put(JsonKey.BATCH_ID, BATCH_ID);
    reqObj.getRequest().put(JsonKey.NAME, NAME);
    reqObj.getRequest().put(JsonKey.START_DATE, "2018-08-17");

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void updateCoursebatchHavingOnlyEndDateAndCourseNotStarted() {

    String courseStatus = "notStarted";
    Response response = getCassandraRecordByBatchId(courseStatus);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_COURSE.getValue());
    reqObj.getRequest().put(JsonKey.BATCH_ID, BATCH_ID);
    reqObj.getRequest().put(JsonKey.NAME, NAME);
    reqObj.getRequest().put(JsonKey.END_DATE, "2018-08-17");

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  // course Ended
  @Test
  public void updateCoursebatchHavingOnlyEndDateAndCourseEnded() {

    String courseStatus = "ended";
    Response response = getCassandraRecordByBatchId(courseStatus);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_COURSE.getValue());
    reqObj.getRequest().put(JsonKey.BATCH_ID, BATCH_ID);
    reqObj.getRequest().put(JsonKey.NAME, NAME);
    reqObj.getRequest().put(JsonKey.END_DATE, "2018-08-17");

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null == res);
  }

  @Test
  public void updateCoursebatchHavingOnlyStartDateAndCourseEnded() {

    String courseStatus = "ended";
    Response response = getCassandraRecordByBatchId(courseStatus);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_COURSE.getValue());
    reqObj.getRequest().put(JsonKey.BATCH_ID, BATCH_ID);
    reqObj.getRequest().put(JsonKey.NAME, NAME);
    reqObj.getRequest().put(JsonKey.START_DATE, "2018-08-17");

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null == res);
  }

  @Test
  public void updateCoursebatchHavingStartAndEndDateAndCourseEnded() {

    String courseStatus = "ended";
    Response response = getCassandraRecordByBatchId(courseStatus);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_COURSE.getValue());
    reqObj.getRequest().put(JsonKey.BATCH_ID, BATCH_ID);
    reqObj.getRequest().put(JsonKey.NAME, NAME);
    reqObj.getRequest().put(JsonKey.START_DATE, "2018-08-17");
    reqObj.getRequest().put(JsonKey.END_DATE, "2018-08-20");
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null == res);
  }

  private Response getCassandraRecordByBatchId(String courseStatus) {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> courseResponseMap = new HashMap<>();

    courseResponseMap.put(JsonKey.ID, "0125706199365795842");
    courseResponseMap.put(JsonKey.VER, "v1");
    courseResponseMap.put(JsonKey.ETS, "2018-08-17 15:21:34:709+0530");

    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(JsonKey.MESSAGE_ID, "8e27cbf5-e299-43b0-bca7-8347f7e5abcf");
    paramMap.put(JsonKey.RES_MSG_ID, "8e27cbf5-e299-43b0-bca7-8347f7e5abcf");
    paramMap.put(JsonKey.ERROR, "null");
    paramMap.put(JsonKey.STATUS, "SUCCESS");
    paramMap.put(JsonKey.ERROR_MSG, "null");

    courseResponseMap.put(JsonKey.PARAMS, paramMap);
    courseResponseMap.put(JsonKey.CODE, "OK");

    Map<String, Object> resultMap = new HashMap<>();
    Map<String, Object> responseMap = new HashMap<>();
    responseMap.put(JsonKey.IDENTIFIER, "123");

    Map<String, Object> courseAdditionInfoMap = new HashMap<>();

    courseAdditionInfoMap.put(JsonKey.COURSE_NAME, "KBC");
    courseAdditionInfoMap.put(JsonKey.LEAF_NODE_COUNT, 1);
    courseAdditionInfoMap.put(JsonKey.DESCRIPTION, "super");
    courseAdditionInfoMap.put(JsonKey.COURSE_LOGO_URL, "http://logo.png");
    courseAdditionInfoMap.put(JsonKey.TOC_URL, "http://toc.png");
    courseAdditionInfoMap.put(JsonKey.STATUS, "Live");
    responseMap.put(JsonKey.COURSE_ADDITIONAL_INFO, courseAdditionInfoMap);
    responseMap.put(JsonKey.END_DATE, "2018-08-17");
    responseMap.put(JsonKey.DESCRIPTION, "Good going");
    responseMap.put(JsonKey.COUNTER_INCREMENT_STATUS, "false");
    responseMap.put(JsonKey.CREATED_DATE, "2018-08-17 17:33:57:233+0530");
    responseMap.put(JsonKey.CREATED_BY, "Kirti");
    responseMap.put(JsonKey.COURSE_CREATOR, "Sagar");
    responseMap.put(JsonKey.HASH_TAG_ID, "0001");
    responseMap.put(JsonKey.NAME, "kirtiTest01");
    responseMap.put(JsonKey.COUNTER_INCREMENT_STATUS, "false");
    responseMap.put(JsonKey.BATCH_ID, "0125706199365795842");
    responseMap.put(JsonKey.ENROLLMENT_TYPE, "Invite-only");
    responseMap.put(JsonKey.COURSE_ID, "do_2125635353836584961866");
    responseMap.put(JsonKey.START_DATE, "2018-08-17");

    if (courseStatus.equals("alreadyStarted")) {
      responseMap.put(JsonKey.STATUS, 1);
    } else if (courseStatus.equals("notStarted")) {
      responseMap.put(JsonKey.STATUS, 0);
    } else {
      responseMap.put(JsonKey.STATUS, 2);
    }
    resultMap.put(JsonKey.RESPONSE, responseMap);
    courseResponseMap.put(JsonKey.RESULT, resultMap);

    list.add(courseResponseMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }
}
