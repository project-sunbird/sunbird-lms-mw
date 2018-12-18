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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.user.actors.EducationManagementActor;
import org.sunbird.user.dao.impl.AddressDaoImpl;
import org.sunbird.user.dao.impl.EducationDaoImpl;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  ElasticSearchUtil.class,
  EncryptionService.class,
  EducationDaoImpl.class,
  AddressDaoImpl.class
})
@PowerMockIgnore({"javax.management.*"})
public class EducationManagementActorTest {

  private static ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(EducationManagementActor.class);
  private static CassandraOperation cassandraOperation;
  Response addrResponse = null;
  private static Map<String, Object> map;

  @BeforeClass
  public static void beforeEachTest() throws Exception {

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    when(cassandraOperation.deleteRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getSuccessResponse());
  }

  @Test
  public void testInsertEducation() {

    when(cassandraOperation.upsertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getUpsertResponse());

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getRequestObject("insertUserEducation", true, true), probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null && res.getResult().get(JsonKey.RESPONSE) == "SUCCESS");
  }

  @Test
  public void testUpdateEducationForDeleteEducationDetailsSuccess() {

    when(cassandraOperation.upsertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getUpsertResponse());

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getRequestObject("updateUserEducation", true, true), probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null && res.getResult().get(JsonKey.RESPONSE) == "SUCCESS");
  }

  @Test
  public void testUpdateEducationWithUpsertEducationDetails() {

    when(cassandraOperation.upsertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getUpsertResponse());

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getRequestObject("updateUserEducation", true, false), probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null && res.getResult().get(JsonKey.RESPONSE) == "SUCCESS");
  }

  @Test
  public void testInsertEducationFailure() {

    when(cassandraOperation.upsertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(new Response());

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getRequestObject("insertUserEducation", true, true), probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null && res.getResult().get(JsonKey.ERROR_MSG) != null);
  }

  @Test
  public void testUpdateEducationForDeleteEducationDetailsFailure() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getRequestObject("updateUserEducation", false, true), probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(res != null && res.getResult().get(JsonKey.ERROR_MSG) != null);
  }

  private Response getUpsertResponse() {

    addrResponse = new Response();
    addrResponse.put(JsonKey.RESPONSE, "SUCCESS");
    return addrResponse;
  }

  private Request getRequestObject(String operation, boolean isParamReq, boolean isDelete) {
    Request reqObj = new Request();
    reqObj.setOperation(operation);
    if (isParamReq) {
      reqObj.put(JsonKey.EDUCATION, getEducationList(isDelete));
      reqObj.put(JsonKey.ID, "someId");
      reqObj.put(JsonKey.CREATED_BY, "createdBy");
    }
    return reqObj;
  }

  private Object getEducationList(boolean isDelete) {

    List<Map<String, Object>> lst = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();

    Map<String, Object> addressmap = new HashMap<>();
    addressmap.put(JsonKey.ID, "someId");
    map.put(JsonKey.ADDRESS, addressmap);
    map.put(JsonKey.ID, "someUserId");
    map.put(JsonKey.IS_DELETED, isDelete);
    lst.add(map);
    return lst;
  }

  private static Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, "SUCCESS");
    return response;
  }
}
