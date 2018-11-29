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
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.KeyCloakConnectionProvider;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.models.user.User;
import org.sunbird.user.actors.UserProfileActor;
import org.sunbird.user.service.impl.UserServiceImpl;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UserServiceImpl.class, KeyCloakConnectionProvider.class, ServiceFactory.class})
@PowerMockIgnore("javax.management.*")
public class UserProfileActorTest {

  private static final Props props = Props.create(UserProfileActor.class);
  private static ActorSystem system = ActorSystem.create("system");
  private String userId = "someUserId";
  private User user;
  private static CassandraOperationImpl cassandraOperation;

  @Before
  public void beforeTest() {

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
  }

  @Test
  public void testGetMediaTypesSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation("getMediaTypes");
    when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getSuccessResponse());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testGetMediaTypesFailure() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    //        when(cassandraOperation.getAllRecords(
    //                Mockito.anyString(), Mockito.anyString()))
    //                .thenThrow(new
    // ProjectCommonException(ResponseCode.SERVER_ERROR.getErrorCode(),
    //                                ResponseCode.SERVER_ERROR.getErrorMessage(),
    //                                ResponseCode.SERVER_ERROR.getResponseCode()));

    Request reqObj = new Request();
    reqObj.setOperation("getMediaTypes");
    subject.tell(reqObj, probe.getRef());

    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null == res);

    //        ProjectCommonException exception =
    //                probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);
    //        Assert.assertTrue(exception
    //                        .getCode()
    //                        .equals(ResponseCode.SERVER_ERROR.getErrorCode()));
  }

  @Test
  public void testInsertUserOrgDetailsSuccessWithoutOrgId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation("insertUserOrgDetails");
    reqObj.put(JsonKey.ROOT_ORG_ID, "ROOT_ORG_ID");
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getSuccessResponse());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUpdateUserOrgDetailsSuccessWithOrgId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation("updateUserOrgDetails");
    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUpdateUserOrgDetailsSuccessWithoutOrgId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation("updateUserOrgDetails");
    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  private Response getSuccessResponse() {
    Response response = new Response();
    List<Map<String, Object>> resMapList = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    resMapList.add(map);
    response.put(JsonKey.RESPONSE, resMapList);
    return response;
  }
}
