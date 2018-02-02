package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.EkStepRequestUtil;
import org.sunbird.learner.util.Util;

/**
 * @author arvind
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(PowerMockRunner.class)
@PrepareForTest(EkStepRequestUtil.class)
@PowerMockIgnore("javax.management.*")
public class CourseEnrollmentActorTest {

  private static ActorSystem system;
  private static final Props props = Props.create(CourseEnrollmentActor.class);
  private static Util.DbInfo userCoursesdbInfo = null;
  private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static String batchId = "115zguf934fy80fui";
  private static final String courseId = "do_212282810555342848180";
  private static Util.DbInfo batchdbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    userCoursesdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    // PowerMockito.mockStatic(EkStepRequestUtil.class);
    insertBatch();
  }

  private static void insertBatch() {

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Map<String, Object> batchMap = new HashMap<String, Object>();
    batchMap.put(JsonKey.ID, batchId);
    batchMap.put(JsonKey.STATUS, 1);
    batchMap.put(JsonKey.COURSE_ID, courseId);
    batchMap.put(JsonKey.CREATED_DATE, (String) format.format(new Date()));
    batchMap.put(JsonKey.START_DATE, (String) format.format(new Date()));
    Calendar now = Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 5);
    Date after5Days = now.getTime();
    batchMap.put(JsonKey.END_DATE, (String) format.format(after5Days));

    cassandraOperation.insertRecord(batchdbInfo.getKeySpace(), batchdbInfo.getTableName(),
        batchMap);
  }

  @Test
  public void testAll() throws Throwable {
    onReceiveTestWithInvalidOperation();
    aonReceiveTestWithInvalidEkStepContent();
    testAonReceive();
    testBEnrollWithSameCourse();
    onReceiveTestWithInvalidRequestType();
    //this.testWithInvalidCourseBatchId();
    Assert.assertTrue(testWithInvalidCourseBatchId() instanceof ProjectCommonException);
  }

  // @Test()
  public void testAonReceive() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setRequestId("1");
    reqObj.setOperation(ActorOperations.ENROLL_COURSE.getValue());
    reqObj.put(JsonKey.COURSE_ID, "do_212282810555342848180");
    reqObj.put(JsonKey.BATCH_ID, batchId);
    reqObj.put(JsonKey.USER_ID, "USR");
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE, reqObj.getRequest());
    innerMap.put(JsonKey.USER_ID, "USR");
    reqObj.setRequest(innerMap);

    PowerMockito.mockStatic(EkStepRequestUtil.class);
    Map<String, Object> ekstepResponse = new HashMap<String, Object>();
    ekstepResponse.put("count", 10);
    Object[] arr = {ekstepResponse};
    Map<String, Object> ekstepMockResult = new HashMap<>();
    ekstepMockResult.put(JsonKey.CONTENTS, arr);
    when(EkStepRequestUtil.searchContent(Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(ekstepMockResult);


    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("100 second"), Response.class);

  }

  // @Test
  public void testBEnrollWithSameCourse() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setRequestId("1");
    reqObj.setOperation(ActorOperations.ENROLL_COURSE.getValue());
    reqObj.put(JsonKey.COURSE_ID, "do_212282810555342848180");
    reqObj.put(JsonKey.USER_ID, "USR");
    reqObj.put(JsonKey.BATCH_ID, batchId);
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE, reqObj.getRequest());
    innerMap.put(JsonKey.USER_ID, "USR");
    reqObj.setRequest(innerMap);

    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
  }

  // @Test
  public void aonReceiveTestWithInvalidEkStepContent() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    PowerMockito.mockStatic(EkStepRequestUtil.class);

    Object[] arr = {};
    Map<String, Object> ekstepMockResult = new HashMap<>();
    ekstepMockResult.put(JsonKey.CONTENTS, arr);
    when(EkStepRequestUtil.searchContent(Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(ekstepMockResult);

    Request reqObj = new Request();
    reqObj.setRequestId("1");
    reqObj.setOperation(ActorOperations.ENROLL_COURSE.getValue());
    reqObj.put(JsonKey.COURSE_ID, "do_212282810555342848180");
    reqObj.put(JsonKey.USER_ID, "USR");
    reqObj.put(JsonKey.BATCH_ID, batchId);
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE, reqObj.getRequest());
    innerMap.put(JsonKey.USER_ID, "USR");
    innerMap.put(JsonKey.COURSE_ID, "do_212282810555342848180");
    reqObj.setRequest(innerMap);


    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
  }

  // @Test()
  public void onReceiveTestWithInvalidOperation() throws Throwable {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setRequestId("1211");
    reqObj.setOperation("INVALID_OPERATION");
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE, reqObj.getRequest());
    innerMap.put(JsonKey.USER_ID, "USR1");
    innerMap.put(JsonKey.ID, "");
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
  }

  // @Test()
  public void onReceiveTestWithInvalidRequestType() throws Throwable {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);


    subject.tell("INVALID REQ", probe.getRef());
    probe.expectMsgClass(ProjectCommonException.class);

  }

  public ProjectCommonException testWithInvalidCourseBatchId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setRequestId("1");
    reqObj.setOperation(ActorOperations.ENROLL_COURSE.getValue());
    reqObj.put(JsonKey.COURSE_ID, "do_212282810555342848180");
    reqObj.put(JsonKey.USER_ID, "USR");
    reqObj.put(JsonKey.BATCH_ID, batchId + 0123);
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE, reqObj.getRequest());
    innerMap.put(JsonKey.USER_ID, "USR");
    reqObj.setRequest(innerMap);

    subject.tell(reqObj, probe.getRef());
    return probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
  }

  @AfterClass
  public static void destroy() {

    cassandraOperation.deleteRecord(userCoursesdbInfo.getKeySpace(),
        userCoursesdbInfo.getTableName(),
        OneWayHashing.encryptVal("USR" + JsonKey.PRIMARY_KEY_DELIMETER + "do_212282810555342848180"
            + JsonKey.PRIMARY_KEY_DELIMETER + batchId));
    cassandraOperation.deleteRecord(batchdbInfo.getKeySpace(), batchdbInfo.getTableName(), batchId);
  }
}
