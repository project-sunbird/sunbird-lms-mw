package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.admin.client.resource.UserResource;
import org.keycloak.admin.client.resource.UsersResource;
import org.keycloak.representations.idm.UserRepresentation;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.KeyCloakConnectionProvider;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.models.user.User;
import org.sunbird.user.actors.UserStatusActor;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UserServiceImpl.class, KeyCloakConnectionProvider.class, ServiceFactory.class})
@PowerMockIgnore("javax.management.*")
public class UserStatusActorTest {

  private static final Props props = Props.create(UserStatusActor.class);
  private static ActorSystem system = ActorSystem.create("system");
  private String userId = "someUserId";
  private User user;
  private static CassandraOperationImpl cassandraOperation;

  @Before
  public void init() {

    PowerMockito.mockStatic(ServiceFactory.class);
    UserResource resource = mock(UserResource.class);
    UsersResource usersResource = mock(UsersResource.class);
    UserRepresentation ur = mock(UserRepresentation.class);
    RealmResource realmResource = mock(RealmResource.class);
    cassandraOperation = mock(CassandraOperationImpl.class);

    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    Keycloak keycloak = mock(Keycloak.class);
    PowerMockito.mockStatic(UserServiceImpl.class);
    UserService userService = mock(UserService.class);
    when(UserServiceImpl.getInstance()).thenReturn(userService);
    PowerMockito.mockStatic(KeyCloakConnectionProvider.class);
    when(KeyCloakConnectionProvider.getConnection()).thenReturn(keycloak);
    when(keycloak.realm(Mockito.anyString())).thenReturn(realmResource);
    when(realmResource.users()).thenReturn(usersResource);
    when(usersResource.get(Mockito.any())).thenReturn(resource);
    when(resource.toRepresentation()).thenReturn(ur);
    ur.setEnabled(Mockito.anyBoolean());
    user = mock(User.class);
    when(userService.getUserById(Mockito.anyString())).thenReturn(user);
  }

  @Test
  public void testBlockUserSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(user.getIsDeleted()).thenReturn(false);
    Response response = createCassandraUpdateSuccessResponse();
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(response);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BLOCK_USER.getValue());
    reqObj.put(JsonKey.USER_ID, userId);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testBlockUserFailureWithUserAlreadyInactive() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(user.getIsDeleted()).thenReturn(true);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BLOCK_USER.getValue());
    reqObj.put(JsonKey.USER_ID, userId);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exception =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.userAlreadyInactive.getErrorCode()));
  }

  @Test
  public void testUnBlockUserSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(user.getIsDeleted()).thenReturn(true);
    Response response = createCassandraUpdateSuccessResponse();
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(response);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UNBLOCK_USER.getValue());
    reqObj.put(JsonKey.USER_ID, userId);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUnBlockUserFailureWithUserAlreadyActive() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(user.getIsDeleted()).thenReturn(false);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UNBLOCK_USER.getValue());
    reqObj.put(JsonKey.USER_ID, userId);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exception =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.userAlreadyActive.getErrorCode()));
  }

  private Response createCassandraUpdateSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }
}
