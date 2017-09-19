package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NotesManagementActorTest {
  
  static ActorSystem system;
  final static Props props = Props.create(NotesManagementActor.class);
  static TestActorRef<NotesManagementActor> ref;
  private static Util.DbInfo usernotesDB = Util.dbInfoMap.get(JsonKey.USER_NOTES_DB);
  private static String userId = "dnk298voopir80249";
  private static String courseId = "mclr309f39";
  private static String contentId = "nnci3u9f97mxcfu";
  private static String noteId = "";
  private static CassandraOperation operation = ServiceFactory.getInstance();
  
  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    ref = TestActorRef.create(system, props, "testActor");
    Util.checkCassandraDbConnections();
    insertUserDataToES();
  }

  private static void insertUserDataToES(){
    Map<String , Object> userMap = new HashMap<>();
    userMap.put(JsonKey.USER_ID , userId);
    userMap.put(JsonKey.FIRST_NAME , "alpha");
    userMap.put(JsonKey.ID , userId);
    userMap.put(JsonKey.ROOT_ORG_ID, "ORG_001");
    userMap.put(JsonKey.USERNAME , "alpha-beta");
    ElasticSearchUtil.createData(EsIndex.sunbird.getIndexName(),EsType.user.getTypeName() , userId , userMap);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void test1CreateNoteSuccess(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> note = new HashMap<>();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY , userId);
    note.put(JsonKey.USER_ID , userId);
    note.put(JsonKey.COURSE_ID , courseId);
    note.put(JsonKey.CONTENT_ID, contentId);
    note.put(JsonKey.NOTE, "This is a test note");
    note.put(JsonKey.TITLE, "Title Test");
    List<String> tags = new ArrayList<>();
    tags.add("tag1");
    note.put(JsonKey.TAGS, tags);
    request.put(JsonKey.NOTE, note);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.CREATE_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"),Response.class);
    noteId = (String)res.getResult().get(JsonKey.ID);
    Assert.assertTrue(!ProjectUtil.isStringNullOREmpty(noteId));
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test2CreateNoteException(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
   
    Request actorMessage = new Request();
    Map<String, Object> note = new HashMap<>();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY , userId+"invalid");
    note.put(JsonKey.USER_ID , userId+"invalid");
    note.put(JsonKey.COURSE_ID , courseId);
    note.put(JsonKey.CONTENT_ID, contentId);
    note.put(JsonKey.NOTE, "This is a test note");
    note.put(JsonKey.TITLE, "Title Test");
    List<String> tags = new ArrayList<>();
    tags.add("tag1");
    note.put(JsonKey.TAGS, tags);
    request.put(JsonKey.NOTE, note);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.CREATE_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res = probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);
    if(null != res){
      Assert.assertEquals(ResponseMessage.Key.INVALID_USER_ID, res.getMessage());
    }
  }
  
  @SuppressWarnings({"deprecation", "unchecked"})
  @Test
  public void test3GetNoteSuccess(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      ProjectLogger.log("Error", e);
    }

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY , userId);
    request.put(JsonKey.NOTE_ID , noteId);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.GET_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"),Response.class);
    if(null!=res && null!= res.getResult()){
      Map<String,Object> response = (Map<String, Object>) res.getResult().get(JsonKey.RESPONSE);
      Long count = (Long) response.get(JsonKey.COUNT);
     Assert.assertTrue(count > 0);
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test4GetNoteException(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY , userId);
    request.put(JsonKey.NOTE_ID , noteId+"invalid");
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.GET_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res= probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);
    if(null != res){
      Assert.assertEquals("Invalid note id", res.getMessage());
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test5UpdateNoteSuccess(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY , userId);
    request.put(JsonKey.NOTE_ID , noteId);
    Map<String,Object> note = new HashMap<>();
    note.put(JsonKey.TITLE, "Title Test Update");
    request.put(JsonKey.NOTE, note);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.UPDATE_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"),Response.class);
    noteId = (String)res.getResult().get(JsonKey.ID);
    Assert.assertTrue(!ProjectUtil.isStringNullOREmpty(noteId));
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test6UpdateNoteException(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY , userId);
    request.put(JsonKey.NOTE_ID , noteId+"invalid");
    Map<String,Object> note = new HashMap<>();
    note.put(JsonKey.TITLE, "Title Test Update");
    request.put(JsonKey.NOTE, note);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.UPDATE_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res= probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);
    if(null != res){
      Assert.assertEquals("Invalid note id", res.getMessage());
    }
  }
  
  @SuppressWarnings({"deprecation", "unchecked"})
  @Test
  public void test7SearchNoteSuccess(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String,Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY , userId);
    
    request.put(JsonKey.QUERY, "note");
    Map<String,Object> filterMap = new HashMap<>();
    filterMap.put(JsonKey.USER_ID, userId);
    request.put(JsonKey.FILTERS, filterMap);
    request.put(JsonKey.OFFSET, 0);
    request.put(JsonKey.LIMIT, 2);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.SEARCH_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"),Response.class);
    if(null!= res && null!= res.getResult()){
       Map<String,Object> response = (Map<String, Object>) res.getResult().get(JsonKey.RESPONSE);
       Long count = (Long) response.get(JsonKey.COUNT);
	  Assert.assertTrue(count > 0);
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test8DeleteNoteSuccess(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String,Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY , userId);
    request.put(JsonKey.NOTE_ID, noteId);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.DELETE_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"),Response.class);

    Assert.assertEquals(200, res.getResponseCode().getResponseCode());
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test9DeleteNoteException(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String,Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY , userId);
    request.put(JsonKey.NOTE_ID , noteId+"invalid");
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.DELETE_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res= probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);
    if(null != res){
      Assert.assertEquals("Invalid note id", res.getMessage());
    }
  }
  
  @AfterClass
  public static void destroy(){

    operation.deleteRecord(usernotesDB.getKeySpace(), usernotesDB.getTableName(), noteId);

    ElasticSearchUtil.removeData(EsIndex.sunbird.getIndexName(),EsType.user.getTypeName() , userId);
    ElasticSearchUtil.removeData(EsIndex.sunbird.getIndexName(),EsType.usernotes.getTypeName() , noteId);
  }
}
