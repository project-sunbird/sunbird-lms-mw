package org.sunbird.learner.actors.recommend;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

/** @author arvind on 27/6/17. */
public class RecommendorActorTest {

  private static ActorSystem system;
  private static final Props props = Props.create(RecommendorActor.class);

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
  }

  @Test()
  public void getRecommendedContents() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setRequestId("1");
    reqObj.setOperation(ActorOperations.GET_RECOMMENDED_COURSES.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, "USR");
    reqObj.setRequest(innerMap);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test()
  public void onReceiveTestWithInvalidOperation() throws Throwable {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setRequestId("1211");
    reqObj.setOperation("INVALID_OPERATION");
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }
}
