package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.user.actors.UserTypeActor;

public class UserTypeActorTest {

  private static ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(UserTypeActor.class);

  @Test
  public void testGetUserTypesSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_USER_TYPES.getValue());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(res.getResponseCode() == ResponseCode.OK && getResponse(res));
  }

  private boolean getResponse(Response res) {

    List<Map<String, Object>> lst =
        (List<Map<String, Object>>) res.getResult().get(JsonKey.RESPONSE);
    if (lst.get(0).get(JsonKey.ID) == "TEACHER"
        && lst.get(1).get(JsonKey.ID) == "OTHER"
        && lst.size() == 2) return true;
    return false;
  }
}
