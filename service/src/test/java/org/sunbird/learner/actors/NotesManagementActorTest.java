package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
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
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

/** Test class to validate the note operations */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NotesManagementActorTest {

  private static ActorSystem system;
  private static final Props props = Props.create(NotesManagementActor.class);
  private static Util.DbInfo usernotesDB = Util.dbInfoMap.get(JsonKey.USER_NOTES_DB);
  private static String userId = "dnk298voopir80249";
  private static String courseId = "mclr309f39";
  private static String contentId = "nnci3u9f97mxcfu";
  private static String noteId = "";
  private static CassandraOperation operation = ServiceFactory.getInstance();

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    insertUserDataToES();
  }

  /** Method to insert User data to ElasticSearch */
  private static void insertUserDataToES() {
    Map<String, Object> userMap = new HashMap<>();
    userMap.put(JsonKey.USER_ID, userId);
    userMap.put(JsonKey.FIRST_NAME, "alpha");
    userMap.put(JsonKey.ID, userId);
    userMap.put(JsonKey.ROOT_ORG_ID, "ORG_001");
    userMap.put(JsonKey.USERNAME, "alpha-beta");
    ElasticSearchUtil.createData(
        EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), userId, userMap);
  }

  /** Method to test create Note when data is valid. Expected to create note without exception */
  @SuppressWarnings("deprecation")
  @Test
  public void test1CreateNoteSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> note = new HashMap<>();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY, userId);
    note.put(JsonKey.USER_ID, userId);
    note.put(JsonKey.COURSE_ID, courseId);
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
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    noteId = (String) res.getResult().get(JsonKey.ID);
    Assert.assertTrue(!StringUtils.isBlank(noteId));
  }

  /** Method to test create Note when data is invalid. Expected to throw exception */
  @SuppressWarnings("deprecation")
  @Test
  public void test2CreateNoteException() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> note = new HashMap<>();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY, userId + "invalid");
    note.put(JsonKey.USER_ID, userId + "invalid");
    note.put(JsonKey.COURSE_ID, courseId);
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
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    if (null != res) {
      Assert.assertEquals(ResponseMessage.Key.INVALID_USER_ID, res.getMessage());
    }
  }

  /**
   * Method to test get Note when noteId is valid. Expected to get note details for the given noteId
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  @Test
  public void test3GetNoteSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      ProjectLogger.log("Error", e);
    }

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY, userId);
    request.put(JsonKey.NOTE_ID, noteId);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.GET_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    if (null != res && null != res.getResult()) {
      Map<String, Object> response = (Map<String, Object>) res.getResult().get(JsonKey.RESPONSE);
      Long count = (Long) response.get(JsonKey.COUNT);
      Assert.assertTrue(count > 0);
    }
  }

  /** Method to test get Note when noteId is invalid. Expected to get exception */
  @SuppressWarnings("deprecation")
  @Test
  public void test4GetNoteException() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY, userId);
    request.put(JsonKey.NOTE_ID, noteId + "invalid");
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.GET_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    if (null != res) {
      Assert.assertEquals("You are not authorized.", res.getMessage());
    }
  }

  /** Method to test update Note when data is valid. Expected to update note with the given data */
  @SuppressWarnings("deprecation")
  @Test
  public void test5UpdateNoteSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY, userId);
    request.put(JsonKey.NOTE_ID, noteId);
    Map<String, Object> note = new HashMap<>();
    note.put(JsonKey.TITLE, "Title Test Update");
    request.put(JsonKey.NOTE, note);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.UPDATE_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    noteId = (String) res.getResult().get(JsonKey.ID);
    Assert.assertTrue(!StringUtils.isBlank(noteId));
  }

  /** Method to test update Note when data is invalid. Expected to throw exception */
  @SuppressWarnings("deprecation")
  @Test
  public void test6UpdateNoteException() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY, userId);
    request.put(JsonKey.NOTE_ID, noteId + "invalid");
    Map<String, Object> note = new HashMap<>();
    note.put(JsonKey.TITLE, "Title Test Update");
    request.put(JsonKey.NOTE, note);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.UPDATE_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    if (null != res) {
      Assert.assertEquals("You are not authorized.", res.getMessage());
    }
  }

  /**
   * Method to test search Note when data is valid. Expected to return note data for the search
   * request
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  @Test
  public void test7SearchNoteSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY, userId);

    request.put(JsonKey.QUERY, "note");
    Map<String, Object> filterMap = new HashMap<>();
    filterMap.put(JsonKey.USER_ID, userId);
    request.put(JsonKey.FILTERS, filterMap);
    request.put(JsonKey.OFFSET, 0);
    request.put(JsonKey.LIMIT, 2);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.SEARCH_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    if (null != res && null != res.getResult()) {
      Map<String, Object> response = (Map<String, Object>) res.getResult().get(JsonKey.RESPONSE);
      Long count = (Long) response.get(JsonKey.COUNT);
      Assert.assertTrue(count > 0);
    }
  }

  /**
   * Method to test delete Note when data is valid. Expected to mark note as deleted and send
   * success message
   */
  @SuppressWarnings("deprecation")
  @Test
  public void test8DeleteNoteSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY, userId);
    request.put(JsonKey.NOTE_ID, noteId);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.DELETE_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);

    Assert.assertEquals(200, res.getResponseCode().getResponseCode());
  }

  /** Method to test delete Note when data is invalid. Expected to throw exception */
  @SuppressWarnings("deprecation")
  @Test
  public void test9DeleteNoteException() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.REQUESTED_BY, userId);
    request.put(JsonKey.NOTE_ID, noteId + "invalid");
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.DELETE_NOTE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    if (null != res) {
      Assert.assertEquals("You are not authorized.", res.getMessage());
    }
  }

  @AfterClass
  public static void destroy() {
    // Delete note data from cassandra
    operation.deleteRecord(usernotesDB.getKeySpace(), usernotesDB.getTableName(), noteId);
    // Delete user and note data from ElasticSearch
    ElasticSearchUtil.removeData(EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), userId);
    ElasticSearchUtil.removeData(
        EsIndex.sunbird.getIndexName(), EsType.usernotes.getTypeName(), noteId);
  }
}
