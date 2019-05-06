package org.sunbird.user;

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
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.PageManagementActor;
import org.sunbird.learner.actors.notificationservice.dao.impl.EmailTemplateDaoImpl;
import org.sunbird.learner.util.ContentSearchUtil;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;
import org.sunbird.user.actors.UserExternalIdManagementActor;
import org.sunbird.user.util.UserActorOperations;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  Util.class,
  DataCacheHandler.class,
  PageManagementActor.class,
  ContentSearchUtil.class,
  org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.class,
  ElasticSearchUtil.class,
  EmailTemplateDaoImpl.class,
  Util.class
})
@PowerMockIgnore({"javax.management.*"})
public class UserExternalIdManagementActorTest {

  private static final Props props = Props.create(UserExternalIdManagementActor.class);
  private ActorSystem system = ActorSystem.create("system");
  private static CassandraOperationImpl cassandraOperation;

  @BeforeClass
  public static void setUp() {

    PowerMockito.mockStatic(ServiceFactory.class);
    PowerMockito.mockStatic(EmailTemplateDaoImpl.class);
    PowerMockito.mockStatic(org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
  }

  private static Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  @Before
  public void beforeTest() {

    PowerMockito.mockStatic(ServiceFactory.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.upsertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(cassandraUpsertRecord());
    when(cassandraOperation.getRecordsByIndexedProperty(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any()))
        .thenReturn(getCassandraRecordsByIndexedProperty());
    cassandraOperation.deleteRecord(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap());
    PowerMockito.mockStatic(Util.class);
    when(Util.encryptData(Mockito.anyString())).thenReturn("userExtId");
  }

  private Response getCassandraRecordsByIndexedProperty() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.ID_TYPE, "anyType");
    response.put(JsonKey.RESPONSE, response);
    return response;
  }

  private Response cassandraUpsertRecord() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  @Test
  public void upsertUserExternalIdentityDetailsSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request request = new Request();
    request.setOperation(UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS.getValue());

    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.OPERATION_TYPE, "CREATE");

    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> extIdMap = new HashMap<>();
    extIdMap.put(JsonKey.OPERATION, "ADD");
    list.add(extIdMap);
    innerMap.put(JsonKey.EXTERNAL_IDS, list);
    request.setRequest(innerMap);

    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void upsertUserExternalIdentityDetailsSuccess2() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request request = new Request();
    request.setOperation(UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS.getValue());

    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.OPERATION_TYPE, "CREATE");

    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> extIdMap = new HashMap<>();
    extIdMap.put(JsonKey.OPERATION, "ADD");
    list.add(extIdMap);
    innerMap.put(JsonKey.EXTERNAL_IDS, list);
    request.setRequest(innerMap);

    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }
}
