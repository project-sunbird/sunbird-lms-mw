package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;

/** @author arvind */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class, ElasticSearchUtil.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class LearnerStateActorTest {

  private static ActorSystem system;
  private static final Props props = Props.create(LearnerStateActor.class);
  private static CassandraOperation cassandraOperation;
  private static String userId = "user121gama";
  private static String courseId = "alpha01crs12";
  private static String courseId2 = "alpha01crs15";
  private static String batchId = "115";
  private static final String contentId = "cont3544TeBuk";

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
  }

  @Before
  public void beforeTest() {
    cassandraOperation = mock(CassandraOperation.class);
    PowerMockito.mockStatic(ServiceFactory.class);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);

    Map<String, Object> esResult = new HashMap<>();
    when(ElasticSearchUtil.complexSearch(
            Mockito.anyObject(), Mockito.anyObject(), Mockito.anyObject()))
        .thenReturn(esResult);

    Response dbResponse = new Response();
    List<Map<String, Object>> dbResponseList = new ArrayList<>();
    dbResponse.put(JsonKey.RESPONSE, dbResponseList);
    when(cassandraOperation.getRecordsByProperty(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyObject()))
        .thenReturn(dbResponse);
    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(dbResponse);
  }

  @Test
  public void testGetCourseByUserId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request request = new Request();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.USER_ID, userId);
    request.setRequest(map);
    request.setOperation(ActorOperations.GET_COURSE.getValue());
    subject.tell(request, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertNotNull(res);
  }

  @Test
  public void testGetCourseWithInvalidOperation() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request request = new Request();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.USER_ID, userId);
    request.setRequest(map);
    request.setOperation("INVALID_OPERATION");
    subject.tell(request, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertNotNull(exc);
  }

  @Test
  public void testForGetContentWithoutUserId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> innerMap = new HashMap<>();
    Request request = new Request();
    innerMap.put(JsonKey.USER_ID, null);
    List<String> contentList = Arrays.asList(contentId);
    innerMap.put(JsonKey.CONTENT_IDS, contentList);
    request.setRequest(innerMap);
    request.setOperation(ActorOperations.GET_CONTENT.getValue());
    subject.tell(request, probe.getRef());
    ProjectCommonException exception =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertNotNull(exception);
    Assert.assertEquals(exception.getResponseCode(), ResponseCode.CLIENT_ERROR.getResponseCode());
  }

  @Test
  public void testContentStateByAllFields() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> innerMap = new HashMap<>();
    Request request = new Request();
    innerMap.put(JsonKey.USER_ID, userId);
    innerMap.put(JsonKey.BATCH_ID, batchId);
    List<String> contentList = Arrays.asList(contentId);
    List<String> courseIds = Arrays.asList(courseId);
    innerMap.put(JsonKey.CONTENT_IDS, contentList);
    innerMap.put(JsonKey.COURSE_IDS, courseIds);
    request.setRequest(innerMap);
    request.setOperation(ActorOperations.GET_CONTENT.getValue());
    subject.tell(request, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertNotNull(res);
  }

  @Test
  public void testContentStateByBatchId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> innerMap = new HashMap<>();
    Request request = new Request();
    innerMap.put(JsonKey.USER_ID, userId);
    innerMap.put(JsonKey.BATCH_ID, batchId);
    request.setRequest(innerMap);
    request.setOperation(ActorOperations.GET_CONTENT.getValue());
    subject.tell(request, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertNotNull(res);
  }

  @Test
  public void testContentStateByOneBatchAndMultipleCourseIds() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> innerMap = new HashMap<>();
    Request request = new Request();
    innerMap.put(JsonKey.USER_ID, userId);
    innerMap.put(JsonKey.BATCH_ID, batchId);
    List<String> courseIds = Arrays.asList(courseId, courseId2);
    innerMap.put(JsonKey.COURSE_IDS, courseIds);
    request.setRequest(innerMap);
    request.setOperation(ActorOperations.GET_CONTENT.getValue());
    subject.tell(request, probe.getRef());
    ProjectCommonException exception =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertNotNull(exception);
    Assert.assertEquals(ResponseCode.CLIENT_ERROR.getResponseCode(), exception.getResponseCode());
  }

  @Test
  public void testForGetContentByCourseIds() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> innerMap = new HashMap<>();
    Request request = new Request();
    innerMap.put(JsonKey.USER_ID, userId);
    List<String> courseList = Arrays.asList(courseId, courseId2);
    innerMap.put(JsonKey.COURSE_IDS, courseList);
    request.setRequest(innerMap);
    request.setOperation(ActorOperations.GET_CONTENT.getValue());
    subject.tell(request, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertNotNull(res);
  }

  @Test
  public void testForGetContentByContentIds() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> innerMap = new HashMap<>();
    Request request = new Request();
    innerMap.put(JsonKey.USER_ID, userId);
    List<String> contentList = Arrays.asList(courseId, courseId2);
    innerMap.put(JsonKey.CONTENT_IDS, contentList);
    request.setRequest(innerMap);
    request.setOperation(ActorOperations.GET_CONTENT.getValue());
    subject.tell(request, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertNotNull(res);
  }
}
