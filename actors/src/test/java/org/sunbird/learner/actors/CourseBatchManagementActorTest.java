package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.Application;
import org.sunbird.learner.util.EkStepRequestUtil;
import org.sunbird.learner.util.Util;


/**
 * @author Amit Kumar.
 */


@RunWith(PowerMockRunner.class)
@PrepareForTest({EkStepRequestUtil.class,ElasticSearchUtil.class,CourseEnrollmentActor.class})
@PowerMockIgnore("javax.management.*")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CourseBatchManagementActorTest {
  
  private static ActorSystem system;
  private static CassandraOperation operation= ServiceFactory.getInstance();
  private static final Props props = Props.create(CourseBatchManagementActor.class);
  private static Util.DbInfo batchDbInfo = null;
  private static Util.DbInfo userOrgdbInfo = null;
  private String courseId = "do_212282810555342848180";
  private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
  private static String batchId = "";
  private static String batchId2 = "";
  private static String hashTagId = "";
  private static String hashTagId2 = "";
  private static Util.DbInfo userOrgDB = null;
  private static String usrOrgId = "";
  private static String userId = "";
  
  @BeforeClass
  public static void setUp() {
      Application.startLocalActorSystem();
      hashTagId = String.valueOf(System.currentTimeMillis());
      hashTagId2 = String.valueOf(System.currentTimeMillis())+45;
      system = ActorSystem.create("system");
      Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
      batchDbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
      userOrgdbInfo = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
      userOrgDB = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
      userId = String.valueOf(System.currentTimeMillis());
      usrOrgId = String.valueOf(System.currentTimeMillis());
  }
  
  @Test
  public void runAllTestCases(){
    createUser();
    test1InvalidOperation();
    test2InvalidMessageType();
    testA1CreateBatch();
    testA2CreateBatch();
    testA1CreateBatchWithInvalidCorsId();
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(),e);
    }
    testB2CreateBatchWithInvalidHashTagId();
    testC3getBatchDetails();
    testA1CreateBatchwithInvalidMentors();
    testA1CreateBatchWithInvalidCourseId();
    testA1CreateBatchWithInvalidOrgId();
    testC3getBatchDetailsWithInvalidId();
    testC4getCourseBatchDetails();
    testC4getCourseBatchDetailsWithInvalidId();
    testD1addUserToBatch();
    testD2addUserToBatchWithInvalidBatchId();
    testE1UpdateBatch();
    testE2UpdateBatchWithExistingHashTagId();
    testE2UpdateBatchAsStartDateBeforeTodayDate();
    testE3UpdateBatchAsStartDateAfterEndDate();
    testE4addUserToBatch();
    testE5addUserToBatch();
    Assert.assertTrue(testE6CreateBatch() instanceof ProjectCommonException);
  }
  
  public void createUser() {
    Map<String,Object> userMap = new HashMap<>();
    userMap.put(JsonKey.USER_ID, userId);
    userMap.put(JsonKey.ORGANISATION_ID, "ORG_001");
    userMap.put(JsonKey.ID, usrOrgId);
    ServiceFactory.getInstance().insertRecord(userOrgdbInfo.getKeySpace(), userOrgdbInfo.getTableName(), userMap); 
    ElasticSearchUtil.createData(ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.user.getTypeName(), userId, userMap);
  }
  
  //@Test
  public void test1InvalidOperation(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation("INVALID_OPERATION");

      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
  }

  //@Test
  public void test2InvalidMessageType(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      subject.tell("Invalid Type", probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
  }
  
  //@Test
  public void testA1CreateBatch(){
    PowerMockito.mockStatic(EkStepRequestUtil.class);
    Map<String , Object> ekstepResponse = new HashMap<String , Object>();
    ekstepResponse.put("count" , 10);
    Object[] arr = {ekstepResponse};
    Map<String,Object> ekstepMockResult = new HashMap<>();
    ekstepMockResult.put(JsonKey.CONTENTS, arr);
    when( EkStepRequestUtil.searchContent(Mockito.anyString() , Mockito.anyMap()) ).thenReturn(ekstepMockResult);
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE_ID, courseId);
    innerMap.put(JsonKey.NAME, "DUMMY_COURSE_NAME1");
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.START_DATE , (String)format.format(new Date()));
    innerMap.put(JsonKey.HASHTAGID ,hashTagId );
    Calendar now =  Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 5);
    Date after5Days = now.getTime();
    innerMap.put(JsonKey.END_DATE , (String)format.format(after5Days));
    List<String> createdFr = new ArrayList<>();
    createdFr.add("ORG_001");
    innerMap.put(JsonKey.COURSE_CREATED_FOR, createdFr);
    List<String> mentors = new ArrayList<>();
    mentors.add(userId);
    innerMap.put(JsonKey.MENTORS, mentors);
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("1000 second"),Response.class);
    batchId = (String) response.getResult().get(JsonKey.BATCH_ID);
  }
  
  public void testA2CreateBatch(){
    PowerMockito.mockStatic(EkStepRequestUtil.class);
    Map<String , Object> ekstepResponse = new HashMap<String , Object>();
    ekstepResponse.put("count" , 10);
    Object[] arr = {ekstepResponse};
    Map<String,Object> ekstepMockResult = new HashMap<>();
    ekstepMockResult.put(JsonKey.CONTENTS, arr);
    when( EkStepRequestUtil.searchContent(Mockito.anyString() , Mockito.anyMap()) ).thenReturn(ekstepMockResult);
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE_ID, courseId);
    innerMap.put(JsonKey.NAME, "DUMMY_COURSE_NAME2");
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.START_DATE , (String)format.format(new Date()));
    innerMap.put(JsonKey.HASHTAGID ,hashTagId2 );
    Calendar now =  Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 5);
    Date after5Days = now.getTime();
    innerMap.put(JsonKey.END_DATE , (String)format.format(after5Days));
    List<String> createdFr = new ArrayList<>();
    createdFr.add("ORG_001");
    innerMap.put(JsonKey.COURSE_CREATED_FOR, createdFr);
    List<String> mentors = new ArrayList<>();
    mentors.add(userId);
    innerMap.put(JsonKey.MENTORS, mentors);
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("1000 second"),Response.class);
    batchId2 = (String) response.getResult().get(JsonKey.BATCH_ID);
  }
  
  public void testA1CreateBatchWithInvalidCorsId(){
    PowerMockito.mockStatic(EkStepRequestUtil.class);
    Map<String , Object> ekstepResponse = new HashMap<String , Object>();
    ekstepResponse.put("count" , 10);
    Object[] arr = {ekstepResponse};
    Map<String,Object> ekstepMockResult = new HashMap<>();
    ekstepMockResult.put(JsonKey.CONTENTS, arr);
    when( EkStepRequestUtil.searchContent(Mockito.anyString() , Mockito.anyMap()) ).thenReturn(ekstepMockResult);
    
    PowerMockito.mockStatic(CourseEnrollmentActor.class);
    Map<String , Object> actorResponse = new HashMap<String , Object>();
    when( CourseEnrollmentActor.getCourseObjectFromEkStep(Mockito.anyString() , Mockito.anyMap()) ).thenReturn(actorResponse);
    
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE_ID, courseId+"789");
    innerMap.put(JsonKey.NAME, "DUMMY_COURSE_NAME1");
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.START_DATE , (String)format.format(new Date()));
    innerMap.put(JsonKey.HASHTAGID ,hashTagId );
    Calendar now =  Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 5);
    Date after5Days = now.getTime();
    innerMap.put(JsonKey.END_DATE , (String)format.format(after5Days));
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  public void testA1CreateBatchWithInvalidCourseId(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE_ID, "12345");
    innerMap.put(JsonKey.NAME, "DUMMY_COURSE_NAME1");
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.START_DATE , (String)format.format(new Date()));
    innerMap.put(JsonKey.HASHTAGID ,hashTagId );
    Calendar now =  Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 5);
    Date after5Days = now.getTime();
    innerMap.put(JsonKey.END_DATE , (String)format.format(after5Days));
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  public void testA1CreateBatchWithInvalidOrgId(){
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE_ID, courseId);
    innerMap.put(JsonKey.NAME, "DUMMY_COURSE_NAME1");
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.START_DATE , (String)format.format(new Date()));
    innerMap.put(JsonKey.HASHTAGID ,hashTagId );
    Calendar now =  Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 5);
    Date after5Days = now.getTime();
    innerMap.put(JsonKey.END_DATE , (String)format.format(after5Days));
    List<String> orgList = new ArrayList<>();
    orgList.add("12589");
    innerMap.put(JsonKey.COURSE_CREATED_FOR, orgList);
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  //@Test
  public void testA1CreateBatchwithInvalidMentors(){
    PowerMockito.mockStatic(EkStepRequestUtil.class);
    Map<String , Object> ekstepResponse = new HashMap<String , Object>();
    ekstepResponse.put("count" , 10);
    Object[] arr = {ekstepResponse};
    Map<String,Object> ekstepMockResult = new HashMap<>();
    ekstepMockResult.put(JsonKey.CONTENTS, arr);
    when( EkStepRequestUtil.searchContent(Mockito.anyString() , Mockito.anyMap()) ).thenReturn(ekstepMockResult);
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE_ID, courseId);
    innerMap.put(JsonKey.NAME, "DUMMY_COURSE_NAME1");
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.START_DATE , (String)format.format(new Date()));
    innerMap.put(JsonKey.HASHTAGID ,hashTagId );
    Calendar now =  Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 5);
    Date after5Days = now.getTime();
    innerMap.put(JsonKey.END_DATE , (String)format.format(after5Days));
    List<String> mentors = new ArrayList<>();
    mentors.add("12589");
    innerMap.put(JsonKey.MENTORS, mentors);
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  //@Test
  public void testB2CreateBatchWithInvalidHashTagId(){
    PowerMockito.mockStatic(EkStepRequestUtil.class);
    Map<String , Object> ekstepResponse = new HashMap<String , Object>();
    ekstepResponse.put("count" , 10);
    Object[] arr = {ekstepResponse};
    Map<String,Object> ekstepMockResult = new HashMap<>();
    ekstepMockResult.put(JsonKey.CONTENTS, arr);
    when( EkStepRequestUtil.searchContent(Mockito.anyString() , Mockito.anyMap()) ).thenReturn(ekstepMockResult);
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE_ID, courseId);
    innerMap.put(JsonKey.NAME, "DUMMY_COURSE_NAME1");
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.START_DATE , (String)format.format(new Date()));
    innerMap.put(JsonKey.HASHTAGID ,hashTagId );
    Calendar now =  Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 5);
    Date after5Days = now.getTime();
    innerMap.put(JsonKey.END_DATE , (String)format.format(after5Days));
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  //@Test
  public void testC3getBatchDetails(){
       
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.BATCH_ID ,batchId );
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("1000 second"),Response.class);
    String hashtagId = (String) ((Map<String,Object>)response.getResult().get(JsonKey.RESPONSE)).get(JsonKey.HASHTAGID);
    assertEquals(true,hashtagId.equalsIgnoreCase(hashTagId));
  }
  
  public void testC3getBatchDetailsWithInvalidId(){
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.BATCH_ID ,batchId+"1234" );
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"),Response.class);
    assertEquals(true,((Map<String, Object>)res.getResult().get(JsonKey.RESPONSE)).isEmpty());
  }
  
  public void testC4getCourseBatchDetails(){
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_COURSE_BATCH_DETAIL.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.BATCH_ID ,batchId );
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("1000 second"),Response.class);
    String hashtagId = (String) (((List<Map<String, Object>>)response.getResult().get(JsonKey.RESPONSE)).get(0)).get(JsonKey.HASHTAGID);
    assertEquals(true,hashtagId.equalsIgnoreCase(hashTagId));
  }
  
 public void testC4getCourseBatchDetailsWithInvalidId(){
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_COURSE_BATCH_DETAIL.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.BATCH_ID ,batchId+"13456" );
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  public void testD1addUserToBatch(){
     TestKit probe = new TestKit(system);
     ActorRef subject = system.actorOf(props);
     Request reqObj = new Request();
     reqObj.setOperation(ActorOperations.ADD_USER_TO_BATCH.getValue());
     HashMap<String, Object> innerMap = new HashMap<>();
     innerMap.put(JsonKey.BATCH_ID ,batchId );
     List<String> userids = new ArrayList<>();
     userids.add(userId);
     innerMap.put(JsonKey.USERIDS ,userids );
     reqObj.getRequest().put(JsonKey.BATCH, innerMap);
     subject.tell(reqObj, probe.getRef());
     probe.expectMsgClass(duration("1000 second"),Response.class);
   }
  
  public void testD2addUserToBatchWithInvalidBatchId(){
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_USER_TO_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.BATCH_ID ,batchId+"1235" );
    List<String> userids = new ArrayList<>();
    userids.add(userId);
    innerMap.put(JsonKey.USERIDS ,userids );
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  public void testE1UpdateBatch(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID ,batchId );
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.HASHTAGID ,""+System.currentTimeMillis());
    List<String> createdFr = new ArrayList<>();
    createdFr.add("ORG_001");
    innerMap.put(JsonKey.COURSE_CREATED_FOR, createdFr);
    List<String> mentors = new ArrayList<>();
    mentors.add(userId);
    innerMap.put(JsonKey.MENTORS, mentors);
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),Response.class);
  }
  
  public void testE2UpdateBatchWithExistingHashTagId(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID ,batchId );
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.HASHTAGID ,hashTagId2);
    List<String> createdFr = new ArrayList<>();
    createdFr.add("ORG_001");
    innerMap.put(JsonKey.COURSE_CREATED_FOR, createdFr);
    List<String> mentors = new ArrayList<>();
    mentors.add(userId);
    innerMap.put(JsonKey.MENTORS, mentors);
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  public void testE2UpdateClosedBatch(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID ,batchId );
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.HASHTAGID ,hashTagId2);
    List<String> createdFr = new ArrayList<>();
    createdFr.add("ORG_001");
    innerMap.put(JsonKey.COURSE_CREATED_FOR, createdFr);
    List<String> mentors = new ArrayList<>();
    mentors.add(userId);
    innerMap.put(JsonKey.MENTORS, mentors);
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  public void testE2UpdateBatchAsStartDateBeforeTodayDate(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID ,batchId );
    Calendar now =  Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, -5);
    Date after5Days = now.getTime();
    innerMap.put(JsonKey.START_DATE , (String)format.format(after5Days));
    innerMap.put(JsonKey.END_DATE , (String)format.format(new Date()));
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  public void testE3UpdateBatchAsStartDateAfterEndDate(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID ,batchId );
    Calendar now =  Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 15);
    Date after5Days = now.getTime();
    innerMap.put(JsonKey.START_DATE , (String)format.format(after5Days));
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("1000 second"),ProjectCommonException.class);
  }
  
  
  public void testE4addUserToBatch(){
    CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    Map<String , Object> request = new HashMap<>();
    request.put(JsonKey.ID, batchId);
    request.put(JsonKey.COURSE_CREATED_FOR, null);
    //request.put(JsonKey.ENROLLMENT_TYPE, null);
    cassandraOperation.updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), request);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_USER_TO_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.BATCH_ID ,batchId );
    List<String> userids = new ArrayList<>();
    userids.add(userId);
    innerMap.put(JsonKey.USERIDS ,userids );
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(ProjectCommonException.class);
  }
  
  public void testE5addUserToBatch(){
    CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    Map<String , Object> request = new HashMap<>();
    request.put(JsonKey.ID, batchId);
    //request.put(JsonKey.COURSE_CREATED_FOR, null);
    request.put(JsonKey.ENROLLMENT_TYPE, null);
    cassandraOperation.updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), request);
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_USER_TO_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.BATCH_ID ,batchId );
    List<String> userids = new ArrayList<>();
    userids.add(userId);
    innerMap.put(JsonKey.USERIDS ,userids );
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(ProjectCommonException.class);
  }
  
  public ProjectCommonException testE6CreateBatch(){
    PowerMockito.mockStatic(EkStepRequestUtil.class);
    Map<String , Object> ekstepResponse = new HashMap<String , Object>();
    ekstepResponse.put("count" , 10);
    Object[] arr = {ekstepResponse};
    Map<String,Object> ekstepMockResult = new HashMap<>();
    ekstepMockResult.put(JsonKey.CONTENTS, arr);
    when( EkStepRequestUtil.searchContent(Mockito.anyString() , Mockito.anyMap()) ).thenReturn(ekstepMockResult);
    
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.COURSE_ID, courseId);
    innerMap.put(JsonKey.NAME, "DUMMY_COURSE_NAME1");
    innerMap.put(JsonKey.ENROLLMENT_TYPE, "invite-only");
    innerMap.put(JsonKey.START_DATE , (String)format.format(new Date()));
    innerMap.put(JsonKey.HASHTAGID ,hashTagId );
    Calendar now =  Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 5);
    Date after5Days = now.getTime();
    innerMap.put(JsonKey.END_DATE , (String)format.format(after5Days));
    List<String> createdFr = new ArrayList<>();
    createdFr.add("ORG_00123456");
    innerMap.put(JsonKey.COURSE_CREATED_FOR, createdFr);
    List<String> mentors = new ArrayList<>();
    mentors.add(userId);
    innerMap.put(JsonKey.MENTORS, mentors);
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);
    subject.tell(reqObj, probe.getRef());
    return probe.expectMsgClass(ProjectCommonException.class);
  }
  
  @AfterClass
  public static void deleteUser() {
    operation.deleteRecord(batchDbInfo.getKeySpace(), batchDbInfo.getTableName(), batchId);
    ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.course.getTypeName(), batchId);
    operation.deleteRecord(batchDbInfo.getKeySpace(), batchDbInfo.getTableName(), batchId2);
    ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.course.getTypeName(), batchId2);
    ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(), userId);
    operation.deleteRecord(userOrgDB.getKeySpace(), userOrgDB.getTableName(), usrOrgId);
    
  }
}
