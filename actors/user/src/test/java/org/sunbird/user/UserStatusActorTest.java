package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.BaseActorTest;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.models.user.User;
import org.sunbird.user.actors.UserStatusActor;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UserServiceImpl.class})
@PowerMockIgnore("javax.management.*")
public class UserStatusActorTest extends BaseActorTest {

  private static final Props props = Props.create(UserStatusActor.class);
  private static final ActorSystem system = ActorSystem.create("system");
  private static boolean staticIsDelete;
  private static final User user = PowerMockito.mock(User.class);

  @Before
  public void init() {
    PowerMockito.mockStatic(UserServiceImpl.class);
    UserService userService = mock(UserService.class);
    when(UserServiceImpl.getInstance()).thenReturn(userService);
    when(userService.getUserById(Mockito.anyString())).thenReturn(user);
  }

  @Test
  public void testUnblockUserFailureWithUserAlreadyActive() {
    boolean result =
        testScenario(
            false,
            ActorOperations.UNBLOCK_USER,
            false,
            ResponseCode.userAlreadyActive.getErrorCode());
    assertTrue(result);
  }

  @Test
  public void testBlockUserSuccess() {
    boolean result = testScenario(false, ActorOperations.BLOCK_USER, true, null);
    assertTrue(result);
  }

  @Test
  public void testBlockUserFailureWithUserAlreadyInactive() {
    boolean result =
        testScenario(
            true,
            ActorOperations.BLOCK_USER,
            false,
            ResponseCode.userAlreadyInactive.getErrorCode());
    assertTrue(result);
  }

  @Test
  public void testUnblockUserSuccess() {
    boolean result = testScenario(true, ActorOperations.UNBLOCK_USER, true, null);
    assertTrue(result);
  }

  private Request getRequestObject(String operation) {

    Request reqObj = new Request();
    String userId = "someUserId";
    reqObj.setOperation(operation);
    reqObj.put(JsonKey.USER_ID, userId);
    return reqObj;
  }

  private boolean testScenario(
      boolean isDeleted,
      ActorOperations operation,
      boolean isSuccess,
      String expectedErrorResponse) {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(user.getIsDeleted()).thenReturn(isDeleted);
    subject.tell(getRequestObject(operation.getValue()), probe.getRef());

    Response res;
    if (isSuccess) {
      res = probe.expectMsgClass(duration("1000 second"), Response.class);
      return (res != null && "SUCCESS".equals(res.getResult().get(JsonKey.RESPONSE)));
    } else {
      ProjectCommonException exception =
          probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);
      return (((ProjectCommonException) exception).getCode().equals(expectedErrorResponse));
    }
  }

  @Override
  protected Map<String, Object> getDataByIdentifierElasticSearch() {
    return null;
  }

  @Override
  protected Response getRecordByIdWithFieldsCassandra() {
    return null;
  }

  @Override
  public Response getRecordByIdCassandra() {
    return null;
  }
}
