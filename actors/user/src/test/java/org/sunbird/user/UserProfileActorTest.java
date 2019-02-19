package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.HashMap;
import java.util.Map;
import org.junit.Ignore;
import org.junit.Test;
import org.sunbird.common.BaseActorTest;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.user.actors.UserProfileActor;

public class UserProfileActorTest extends BaseActorTest {

  private ActorSystem system = ActorSystem.create("system");
  private static final String userId = "USER-ID";
  private final Props props = Props.create(UserProfileActor.class);
  private static boolean isSuccessStatic;

  @Test
  @Ignore
  public void testSetProfileVisibilityFailure() {

    boolean result =
        testScenario(
            ActorOperations.PROFILE_VISIBILITY, false, ResponseCode.userNotFound.getErrorCode());
    assertTrue(result);
  }

  @Test
  @Ignore
  public void testGetMediaTypesSuccess() {

    boolean result = testScenario(ActorOperations.GET_MEDIA_TYPES, true, null);
    assertTrue(result);
  }

  @Test
  @Ignore
  public void testSetProfileVisibilitySuccess() {

    boolean result = testScenario(ActorOperations.PROFILE_VISIBILITY, true, null);
    assertTrue(result);
  }

  private boolean testScenario(
      ActorOperations actorOperation, boolean isSuccess, String expectedErrorResponse) {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    isSuccessStatic = isSuccess;
    subject.tell(getRequestedObj(actorOperation), probe.getRef());

    if (isSuccess) {
      Response res = probe.expectMsgClass(duration("10 second"), Response.class);
      return null != res && res.getResponseCode() == ResponseCode.OK;
    }
    ProjectCommonException exception =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    return (((ProjectCommonException) exception).getCode().equals(expectedErrorResponse));
  }

  private Object getRequestedObj(ActorOperations actorOperation) {

    Request reqObj = new Request();
    reqObj.put(JsonKey.USER_ID, userId);
    reqObj.setOperation(actorOperation.getValue());
    return reqObj;
  }

  private static HashMap getEsResponseMap() {
    HashMap<String, Object> map = new HashMap<>();
    map.put(JsonKey.CONTENT, "Any-content");
    return map;
  }

  @Override
  protected Map<String, Object> getDataByIdentifierElasticSearch() {
    if (isSuccessStatic) {
      return getEsResponseMap();
    } else return new HashMap<>();
  }

  @Override
  protected Response getRecordByIdWithFieldsCassandra() {
    return null;
  }

  @Override
  protected Response getRecordByIdCassandra() {
    return null;
  }
}
