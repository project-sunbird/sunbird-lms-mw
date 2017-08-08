package org.sunbird.learner.actors;

import java.sql.DriverManager;
import java.util.Date;
import java.util.HashMap;


import static org.junit.Assert.assertEquals;

import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Before;
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
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.EkStepRequestUtil;
import org.sunbird.learner.util.Util;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import static akka.testkit.JavaTestKit.duration;
import static org.mockito.Matchers.anyObject;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * @author arvind
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(PowerMockRunner.class)
@PrepareForTest(EkStepRequestUtil.class)
@PowerMockIgnore("javax.management.*")
public class CourseEnrollmentActorTest {


  static ActorSystem system;
  final static  Props props = Props.create(CourseEnrollmentActor.class);
  static Util.DbInfo userCoursesdbInfo = null;
  private static CassandraOperation cassandraOperation = new CassandraOperationImpl();
  private static String batchId="115";

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections();
    userCoursesdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    //PowerMockito.mockStatic(EkStepRequestUtil.class);
  }

  @Test
  public void testAll() throws Throwable {
    this.onReceiveTestWithInvalidOperation();
    this.aonReceiveTestWithInvalidEkStepContent();
    this.testAonReceive();
    this.testBEnrollWithSameCourse();
    this.onReceiveTestWithInvalidRequestType();
  }

  //@Test()
  public void testAonReceive() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setRequest_id("1");
    reqObj.setOperation(ActorOperations.ENROLL_COURSE.getValue());
    reqObj.put(JsonKey.COURSE_ID, "do_212282810555342848180");
    reqObj.put(JsonKey.BATCH_ID,batchId);
    reqObj.put(JsonKey.USER_ID, "USR");
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE, reqObj.getRequest());
    innerMap.put(JsonKey.USER_ID, "USR");
    reqObj.setRequest(innerMap);

    PowerMockito.mockStatic(EkStepRequestUtil.class);
    Map<String , Object> ekstepResponse = new HashMap<String , Object>();
    ekstepResponse.put("count" , 10);
    Object[] ekstepMockResult = {ekstepResponse};
    when( EkStepRequestUtil.searchContent(Mockito.anyString() , Mockito.anyMap()) ).thenReturn(ekstepMockResult);


    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("100 second"),Response.class);

  }

  //@Test
  public void testBEnrollWithSameCourse() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setRequest_id("1");
    reqObj.setOperation(ActorOperations.ENROLL_COURSE.getValue());
    reqObj.put(JsonKey.COURSE_ID, "do_212282810555342848180");
    reqObj.put(JsonKey.USER_ID, "USR");
    reqObj.put(JsonKey.BATCH_ID,batchId);
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE, reqObj.getRequest());
    innerMap.put(JsonKey.USER_ID, "USR");
    reqObj.setRequest(innerMap);

    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
  }

  //@Test
  public void aonReceiveTestWithInvalidEkStepContent(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    PowerMockito.mockStatic(EkStepRequestUtil.class);

    Object[] ekstepMockResult = {};
    when( EkStepRequestUtil.searchContent(Mockito.anyString() , Mockito.anyMap()) ).thenReturn(ekstepMockResult);

    Request reqObj = new Request();
    reqObj.setRequest_id("1");
    reqObj.setOperation(ActorOperations.ENROLL_COURSE.getValue());
    reqObj.put(JsonKey.COURSE_ID, "do_212282810555342848180");
    reqObj.put(JsonKey.USER_ID, "USR");
    reqObj.put(JsonKey.BATCH_ID,batchId);
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE, reqObj.getRequest());
    innerMap.put(JsonKey.USER_ID, "USR");
    innerMap.put(JsonKey.COURSE_ID ,"do_212282810555342848180" );
    reqObj.setRequest(innerMap);


    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
  }

  //@Test()
  public void onReceiveTestWithInvalidOperation() throws Throwable {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setRequest_id("1211");
    reqObj.setOperation("INVALID_OPERATION");
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE, reqObj.getRequest());
    innerMap.put(JsonKey.USER_ID, "USR1");
    innerMap.put(JsonKey.ID, "");
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
  }

  //@Test()
  public void onReceiveTestWithInvalidRequestType() throws Throwable {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);


    subject.tell("INVALID REQ", probe.getRef());
    probe.expectMsgClass( ProjectCommonException.class);

  }

  @AfterClass
  public static void destroy(){

    cassandraOperation.deleteRecord(userCoursesdbInfo.getKeySpace(), userCoursesdbInfo.getTableName(), OneWayHashing.encryptVal("USR"+ JsonKey.PRIMARY_KEY_DELIMETER+"do_212282810555342848180"+JsonKey.PRIMARY_KEY_DELIMETER+batchId));
  }
}
