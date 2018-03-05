package org.sunbird.learner.actors;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.Application;
import org.sunbird.learner.actors.notificationservice.EmailServiceActor;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;

public class EmailServiceActorTest {

  private static ActorSystem system;
  private final static Props props = Props.create(EmailServiceActor.class);

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
      ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
      Assert.assertTrue(null != exc);
  }
  
  @Test
  public void testInvalidRequestData(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Response reqObj = new Response();

      subject.tell(reqObj, probe.getRef());
      ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
      Assert.assertTrue(null != exc);
  }
}
