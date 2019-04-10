package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
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
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ServiceFactory.class, ElasticSearchUtil.class, Util.class, RequestRouter.class,
    ElasticSearchUtil.class })
@PowerMockIgnore({ "javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*" })
public class NotesManagementActorTest {

  private static String userId = "userId-example";
  private static String noteId = "";
  private ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(NotesManagementActor.class);
  private static CassandraOperationImpl cassandraOperation;

  @Before
  public void beforeEachTest() {
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
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap);
    when(cassandraOperation.insertRecord(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
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

  @Test
  public void testCreateNoteFailureWithInvalidUserId() {
    Request req = new Request();
    Map<String, Object> reqMap = new HashMap<>();
    req.setRequest(reqMap);
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Mockito.anyMap());
    req.setOperation(ActorOperations.CREATE_NOTE.getValue());
    boolean result = testScenario(req, ResponseCode.invalidUserId);
    assertTrue(result);
  }

  @Test
  public void testUpdateNoteFailure() {
    Request req = new Request();
    req.getContext().put(JsonKey.USER_ID, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    Map<String, Object> reqMap = new HashMap<>();
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.UPDATE_NOTE.getValue());
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Mockito.anyMap());
    boolean result = testScenario(req, ResponseCode.unAuthorized);
    assertTrue(result);
  }

  @Test
  public void testUpdateNoteFailurewithUserIdMismatch() {
    Request req = new Request();
    req.getContext().put(JsonKey.REQUESTED_BY, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.USER_ID, "misMatch");
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.UPDATE_NOTE.getValue());
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap);
    boolean result = testScenario(req, ResponseCode.errorForbidden);
    assertTrue(result);
  }

  @Test
  public void testUpdateNoteFailurewithEmptyNote() {
    Request req = new Request();
    req.getContext().put(JsonKey.REQUESTED_BY, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.USER_ID, userId);
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.UPDATE_NOTE.getValue());
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap).thenReturn(Mockito.anyMap());
    boolean result = testScenario(req, ResponseCode.invalidNoteId);
    assertTrue(result);
  }

  @Test
  public void testUpdateNoteSuccess() {
    Request req = new Request();
    req.getContext().put(JsonKey.REQUESTED_BY, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.USER_ID, userId);
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.UPDATE_NOTE.getValue());
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap).thenReturn(reqMap);
    when(cassandraOperation.updateRecord(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    boolean result = testScenario(req, null);
    assertTrue(result);
  }

  @Test
  public void testSearchNoteSuccess() {
    Request req = new Request();
    req.getContext().put(JsonKey.REQUESTED_BY, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    Map<String, Object> reqMap = new HashMap<>();
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.SEARCH_NOTE.getValue());
    when(ElasticSearchUtil.complexSearch(Mockito.any(), Mockito.anyString(), Mockito.anyString())).thenReturn(reqMap);
    boolean result = testScenario(req, null);
    assertTrue(result);
  }

  @Test
  public void testGetNoteFailurewithUserIdMismatch() {
    Request req = new Request();
    req.getContext().put(JsonKey.REQUESTED_BY, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.USER_ID, "misMatch");
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.GET_NOTE.getValue());
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap);
    boolean result = testScenario(req, ResponseCode.errorForbidden);
    assertTrue(result);
  }

  @Test
  public void testGetNoteFailureWithInvalidUserId() {
    Request req = new Request();
    Map<String, Object> reqMap = new HashMap<>();
    req.setRequest(reqMap);
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Mockito.anyMap());
    req.setOperation(ActorOperations.GET_NOTE.getValue());
    boolean result = testScenario(req, ResponseCode.invalidParameterValue);
    assertTrue(result);
  }

  @Test
  public void testGetNoteFailureWithInvalidNoteId() {
    Request req = new Request();
    Map<String, Object> reqMap = new HashMap<>();
    req.getContext().put(JsonKey.REQUESTED_BY, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    reqMap.put(JsonKey.USER_ID, userId);
    reqMap.put(JsonKey.COUNT, 0L);
    req.setRequest(reqMap);
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap);
    when(ElasticSearchUtil.complexSearch(Mockito.any(), Mockito.anyString(), Mockito.anyString())).thenReturn(reqMap);
    req.setOperation(ActorOperations.GET_NOTE.getValue());
    boolean result = testScenario(req, ResponseCode.invalidNoteId);
    assertTrue(result);
  }

  @Test
  public void testGetNoteSuccess() {
    Request req = new Request();
    Map<String, Object> reqMap = new HashMap<>();
    req.getContext().put(JsonKey.REQUESTED_BY, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    reqMap.put(JsonKey.USER_ID, userId);
    reqMap.put(JsonKey.COUNT, 1L);
    req.setRequest(reqMap);
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap);
    when(ElasticSearchUtil.complexSearch(Mockito.any(), Mockito.anyString(), Mockito.anyString())).thenReturn(reqMap);
    req.setOperation(ActorOperations.GET_NOTE.getValue());
    boolean result = testScenario(req, null);
    assertTrue(result);
  }

  @Test
  public void testDeleteNoteSuccess() {
    Request req = new Request();
    req.getContext().put(JsonKey.REQUESTED_BY, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.USER_ID, userId);
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.DELETE_NOTE.getValue());
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap).thenReturn(reqMap);
    when(cassandraOperation.updateRecord(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    boolean result = testScenario(req, null);
    assertTrue(result);
  }

  @Test
  public void testDeleteNoteFailurewithEmptyNote() {
    Request req = new Request();
    req.getContext().put(JsonKey.REQUESTED_BY, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.USER_ID, userId);
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.DELETE_NOTE.getValue());
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap).thenReturn(Mockito.anyMap());
    boolean result = testScenario(req, ResponseCode.invalidNoteId);
    assertTrue(result);
  }

  @Test
  public void testDeleteNoteFailurewithUserIdMismatch() {
    Request req = new Request();
    req.getContext().put(JsonKey.REQUESTED_BY, userId);
    req.getContext().put(JsonKey.NOTE_ID, noteId);
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.USER_ID, "misMatch");
    req.setRequest(reqMap);
    req.setOperation(ActorOperations.DELETE_NOTE.getValue());
    when(ElasticSearchUtil.getDataByIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(reqMap);
    boolean result = testScenario(req, ResponseCode.errorForbidden);
    assertTrue(result);
  }

  private boolean testScenario(Request reqObj, ResponseCode errorCode) {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(reqObj, probe.getRef());

    if (errorCode == null) {
      Response res = probe.expectMsgClass(duration("10 second"), Response.class);
      return null != res && res.getResponseCode() == ResponseCode.OK;
    } else {
      ProjectCommonException res = probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
      return res.getCode().equals(errorCode.getErrorCode()) || res.getResponseCode() == errorCode.getResponseCode();
    }
  }

  private static Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }
}
