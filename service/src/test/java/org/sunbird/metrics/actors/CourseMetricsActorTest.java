package org.sunbird.metrics.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.actor.service.SunbirdMWService;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

/** Created by arvind on 8/9/17. */
public class CourseMetricsActorTest {

  private static ActorSystem system;
  private static final Props props = Props.create(CourseMetricsActor.class);
  private static String userId = "dnk298voopir80249";
  private static String batchId = "jkwf6t3r083fp4h";
  private static final String orgId = "vdckcyigc68569";

  @BeforeClass
  public static void setUp() {
    SunbirdMWService.init();
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    insertBatchDataToES();
    insertOrgDataToES();
    insertUserCoursesDataToES();
    insertUserDataToES();
  }

  private static void insertUserDataToES() {
    Map<String, Object> userMap = new HashMap<>();
    userMap.put(JsonKey.USER_ID, userId);
    userMap.put(JsonKey.FIRST_NAME, "alpha");
    userMap.put(JsonKey.ID, userId);
    userMap.put(JsonKey.ROOT_ORG_ID, "ORG_001");
    userMap.put(JsonKey.USERNAME, "alpha-beta");
    userMap.put(JsonKey.REGISTERED_ORG_ID, orgId);
    userMap.put(JsonKey.EMAIL, "arvind.glaitm108@gmail.com");
    ElasticSearchUtil.createData(
        EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), userId, userMap);
  }

  private static void insertBatchDataToES() {
    Map<String, Object> batchMap = new HashMap<>();
    batchMap.put(JsonKey.ID, batchId);
    ElasticSearchUtil.createData(
        EsIndex.sunbird.getIndexName(), EsType.course.getTypeName(), batchId, batchMap);
  }

  private static void insertUserCoursesDataToES() {
    Map<String, Object> userCoursesMap = new HashMap<>();
    userCoursesMap.put(JsonKey.ID, batchId + JsonKey.PRIMARY_KEY_DELIMETER + userId);
    userCoursesMap.put(JsonKey.BATCH_ID, batchId);
    userCoursesMap.put(JsonKey.USER_ID, userId);
    userCoursesMap.put(JsonKey.REGISTERED_ORG_ID, orgId);
    userCoursesMap.put(JsonKey.PROGRESS, 1);
    ElasticSearchUtil.createData(
        EsIndex.sunbird.getIndexName(),
        EsType.usercourses.getTypeName(),
        batchId + JsonKey.PRIMARY_KEY_DELIMETER + userId,
        userCoursesMap);
  }

  private static void insertOrgDataToES() {
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, orgId);
    orgMap.put(JsonKey.ORGANISATION_NAME, "IIM");
    orgMap.put(JsonKey.ORGANISATION_ID, orgId);
    ElasticSearchUtil.createData(
        EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName(), batchId, orgMap);
  }

  @Test
  public void testCourseProgress() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.put(JsonKey.BATCH_ID, batchId);
    actorMessage.put(JsonKey.PERIOD, "fromBegining");
    actorMessage.setOperation(ActorOperations.COURSE_PROGRESS_METRICS.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.PERIOD));
  }

  @Test
  public void testUnsupportedMessageType() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    subject.tell("Invalid Oject Type", probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testCourseProgress001() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.put(JsonKey.BATCH_ID, batchId);
    actorMessage.put(JsonKey.PERIOD, "7d");
    actorMessage.setOperation(ActorOperations.COURSE_PROGRESS_METRICS.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.PERIOD));
  }

  @Test
  public void testCourseProgressWithInvalidBatch() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.put(JsonKey.BATCH_ID, batchId + "8ud38cuy");
    actorMessage.put(JsonKey.PERIOD, "fromBegining");
    actorMessage.setOperation(ActorOperations.COURSE_PROGRESS_METRICS.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testCourseProgressWithInvalidBatchIdNull() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.put(JsonKey.BATCH_ID, null);
    actorMessage.put(JsonKey.PERIOD, "fromBegining");
    actorMessage.setOperation(ActorOperations.COURSE_PROGRESS_METRICS.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testCourseProgressWithInvalidOperationName() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.put(JsonKey.BATCH_ID, null);
    actorMessage.put(JsonKey.PERIOD, "fromBegining");
    actorMessage.setOperation(ActorOperations.COURSE_PROGRESS_METRICS.getValue() + "-Invalid");

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testCourseProgressReport() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.put(JsonKey.BATCH_ID, batchId);
    actorMessage.put(JsonKey.PERIOD, "fromBegining");
    actorMessage.put(JsonKey.FORMAT, "csv");
    actorMessage.setOperation(ActorOperations.COURSE_PROGRESS_METRICS_REPORT.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.REQUEST_ID));
  }

  @Test
  public void testCourseProgressReport001() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.put(JsonKey.BATCH_ID, batchId);
    actorMessage.put(JsonKey.PERIOD, "7d");
    actorMessage.put(JsonKey.FORMAT, "csv");
    actorMessage.setOperation(ActorOperations.COURSE_PROGRESS_METRICS_REPORT.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.REQUEST_ID));
  }

  @Test
  public void testCourseProgressReportWithInvalidBatch() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.put(JsonKey.BATCH_ID, batchId + "ug8er73");
    actorMessage.put(JsonKey.PERIOD, "fromBegining");
    actorMessage.put(JsonKey.FORMAT, "csv");
    actorMessage.setOperation(ActorOperations.COURSE_PROGRESS_METRICS_REPORT.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testCourseProgressReportWithInvalidBatchIdNull() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.put(JsonKey.BATCH_ID, "");
    actorMessage.put(JsonKey.PERIOD, "fromBegining");
    actorMessage.put(JsonKey.FORMAT, "csv");
    actorMessage.setOperation(ActorOperations.COURSE_PROGRESS_METRICS_REPORT.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  @Test
  public void testCourseConsumption() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.COURSE_ID, "mclr309f39");
    actorMessage.put(JsonKey.PERIOD, "7d");
    actorMessage.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.setOperation(ActorOperations.COURSE_CREATION_METRICS.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("1500 second"), Response.class);
    Map<String, Object> data = res.getResult();
    Assert.assertEquals("7d", data.get(JsonKey.PERIOD));
    Assert.assertEquals(
        "mclr309f39", ((Map<String, Object>) data.get("course")).get(JsonKey.COURSE_ID));
    Map<String, Object> series = (Map<String, Object>) data.get(JsonKey.SERIES);
    Assert.assertTrue(series.containsKey("course.consumption.time_spent"));
    Assert.assertTrue(series.containsKey("course.consumption.content.users.count"));
    List<Map<String, Object>> buckets =
        (List<Map<String, Object>>)
            ((Map<String, Object>) series.get("course.consumption.content.users.count"))
                .get("buckets");
    Assert.assertEquals(7, buckets.size());
    Map<String, Object> snapshot = (Map<String, Object>) data.get(JsonKey.SNAPSHOT);
    Assert.assertTrue(snapshot.containsKey("course.consumption.time_spent.count"));
    Assert.assertTrue(snapshot.containsKey("course.consumption.time_per_user"));
    Assert.assertTrue(snapshot.containsKey("course.consumption.users_completed"));
    Assert.assertTrue(snapshot.containsKey("course.consumption.time_spent_completion_count"));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testCourseConsumptionInvalidUserData() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.COURSE_ID, "mclr309f39_INVALID");
    actorMessage.put(JsonKey.PERIOD, "7d");
    actorMessage.put(JsonKey.REQUESTED_BY, userId + "Invalid");
    actorMessage.setOperation(ActorOperations.COURSE_CREATION_METRICS.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException e =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertEquals("UNAUTHORIZE_USER", e.getCode());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testCourseConsumptionInvalidPeriod() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.COURSE_ID, "mclr309f39");
    actorMessage.put(JsonKey.PERIOD, "10d");
    actorMessage.setOperation(ActorOperations.COURSE_CREATION_METRICS.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException e =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertEquals("INVALID_PERIOD", e.getCode());
  }
}
