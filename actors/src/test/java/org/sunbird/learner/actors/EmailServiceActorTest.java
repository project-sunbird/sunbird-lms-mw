package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.Application;
import org.sunbird.learner.actors.notificationservice.EmailServiceActor;
import org.sunbird.learner.util.Util;

public class EmailServiceActorTest {

  static ActorSystem system;
  final static Props props = Props.create(EmailServiceActor.class);
  String courseId = "";

  @BeforeClass
  public static void setUp() {
      Application.startLocalActorSystem();
      system = ActorSystem.create("system");
      Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
  }

  
  @Test
  public void testInvalidOperation(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation("INVALID_OPERATION");

      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
  }
  
  @Test
  public void testInvalidRequestData(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Response reqObj = new Response();

      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
  }
}
