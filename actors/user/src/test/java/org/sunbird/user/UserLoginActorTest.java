package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.Assert;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.user.actors.UserLoginActor;

public class UserLoginActorTest {

  private static final Props props = Props.create(UserLoginActor.class);
  private static ActorSystem system = ActorSystem.create("system");
  private String userId = "someUserId";
  TestKit probe = new TestKit(system);
  ActorRef subject = system.actorOf(props);

  @Test
  public void testUpdateUserLoginTimeSuccess() {

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.USER_CURRENT_LOGIN.getValue());
    reqObj.put(JsonKey.USER_ID, userId);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUpdateUserLoginTimeFailureWithInvalidMessage() {

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_USER_COUNT.getValue());
    reqObj.put(JsonKey.USER_ID, userId);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exception =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.invalidRequestData.getErrorCode()));
  }
}
