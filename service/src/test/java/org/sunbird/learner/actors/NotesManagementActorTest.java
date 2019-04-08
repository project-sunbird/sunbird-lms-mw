package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
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
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  ElasticSearchUtil.class,
  Util.class,
  RequestRouter.class,
  ElasticSearchUtil.class
})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*"})
public class NotesManagementActorTest {

  private static Util.DbInfo usernotesDB = Util.dbInfoMap.get(JsonKey.USER_NOTES_DB);
  private static String userId = "userId-example";
  private static String courseId = "mclr309f39";
  private static String contentId = "nnci3u9f97mxcfu";
  private static String noteId = "";

  private ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(NotesManagementActor.class);
  private static CassandraOperationImpl cassandraOperation;

  @BeforeClass
  public static void setUp() {

    ActorRef actorRef = mock(ActorRef.class);
    PowerMockito.mockStatic(RequestRouter.class);
    when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);

    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
  }

  @Test
  public void testCreateNoteSuccess() {
    Request req = new Request();
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.USER_ID, userId);
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.CREATE_NOTE.getValue());
    when(ElasticSearchUtil.getDataByIdentifier(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap);
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    boolean result = testScenario(req, null);
    assertTrue(result);
  }

  @Test
  public void testCreateNoteFailure() {
    Request req = new Request();
    Map<String, Object> reqMap = new HashMap<>();
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.CREATE_NOTE.getValue());
    boolean result = testScenario(req, ResponseCode.invalidUserId);
    assertTrue(result);
  }

  /*
   * private void mockCassandra() {
   *
   * when(cassandraOperation.insertRecord( Mockito.anyString(),
   * Mockito.anyString(), Mockito.anyMap())) .thenReturn(); }
   */

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

  @SuppressWarnings("deprecation")
  @Ignore
  @Test
  public void checkTelemetryKeyFailure() throws Exception {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    String telemetryEnvKey = "user";
    PowerMockito.mockStatic(Util.class);
    PowerMockito.doNothing()
        .when(
            Util.class,
            "initializeContext",
            Mockito.any(Request.class),
            Mockito.eq(telemetryEnvKey));

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
    Assert.assertTrue(!(telemetryEnvKey.charAt(0) >= 65 && telemetryEnvKey.charAt(0) <= 90));
  }

  /** Method to test create Note when data is valid. Expected to create note without exception */
  @SuppressWarnings("deprecation")
  @Ignore
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

  /**
   * Method to test delete Note when data is valid. Expected to mark note as deleted and send
   * success message
   */
  @SuppressWarnings("deprecation")
  @Test
  @Ignore
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
  @Ignore
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

  private boolean testScenario(Request reqObj, ResponseCode errorCode) {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(reqObj, probe.getRef());

    if (errorCode == null) {
      Response res = probe.expectMsgClass(duration("10 second"), Response.class);
      return null != res && res.getResponseCode() == ResponseCode.OK;
    } else {
      ProjectCommonException res =
          probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
      return res.getCode().equals(errorCode.getErrorCode())
          || res.getResponseCode() == errorCode.getResponseCode();
    }
  }

  private static Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }
}
