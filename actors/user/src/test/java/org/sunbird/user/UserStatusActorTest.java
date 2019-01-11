package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.sunbird.actor.core.BaseActorTest;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.user.actors.UserStatusActor;

public class UserStatusActorTest extends BaseActorTest {

  private static final Props props = Props.create(UserStatusActor.class);
  private static final ActorSystem system = ActorSystem.create("system");

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

  @Test
  public void testUnblockUserFailureWithUserAlreadyActive() {
    resetAllMocks();
    boolean result =
        testScenario(
            false,
            ActorOperations.UNBLOCK_USER,
            false,
            ResponseCode.userAlreadyActive.getErrorCode());
    assertTrue(result);
  }

  private void resetAllMocks() {}

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
    //    getMapResponse(isDeleted);
    getCassandraResponseForId(isDeleted);
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
  protected Response getRecordByIdResponseAbstract(boolean isDeleted) {

    Response response = new Response();
    List<Map<String, Object>> resMapList = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.IS_DELETED, isDeleted);
    resMapList.add(map);
    response.put(JsonKey.RESPONSE, resMapList);
    return response;
  }

  /*@Override
  protected Map<String, Object> getMapResponse(boolean b) {
    return null;
  }*/

  //  @Override
  //  protected Map<String, Object> getMapResponse(boolean isDeleted) {
  //    Map<String, Object> map = new HashMap<>();
  //    map.put(JsonKey.IS_DELETED, isDeleted);
  //    return map;
  //  }

  @Override
  protected Map<String, Object> esComplexSearchResponse() {
    return null;
  }

  /*@Override
  public void getRecordByIdResponse(boolean isDelete) {

  }*/
}
