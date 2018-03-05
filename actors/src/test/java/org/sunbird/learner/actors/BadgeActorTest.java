package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.Application;
import org.sunbird.learner.actors.badges.BadgesActor;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;

public class BadgeActorTest {

	private static ActorSystem system;
	private static final Props props = Props.create(BadgesActor.class);

	@BeforeClass
	public static void setUp() {
		Application.startLocalActorSystem();
		system = ActorSystem.create("system");
		Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
	}

	@Test
	public void getAllBadges() {
		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);

		Request reqObj = new Request();
		reqObj.setOperation(ActorOperations.GET_ALL_BADGE.getValue());
		subject.tell(reqObj, probe.getRef());
		Response res = probe.expectMsgClass(duration("200 second"), Response.class);
		Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
	}

	@Test
	public void getHealthCheck() {
		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);

		Request reqObj = new Request();
		reqObj.setOperation(ActorOperations.HEALTH_CHECK.getValue());
		subject.tell(reqObj, probe.getRef());
		Response res = probe.expectMsgClass(duration("200 second"), Response.class);
		Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
	}

	@Test
	public void getACTORHealthCheck() {
		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);

		Request reqObj = new Request();
		reqObj.setOperation(ActorOperations.ACTOR.getValue());
		subject.tell(reqObj, probe.getRef());
		Response res = probe.expectMsgClass(duration("200 second"), Response.class);
		Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
	}

	@Test
	public void getESHealthCheck() {
		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);

		Request reqObj = new Request();
		reqObj.setOperation(ActorOperations.ES.getValue());
		subject.tell(reqObj, probe.getRef());
		Response res = probe.expectMsgClass(duration("200 second"), Response.class);
		Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
	}

	@Test
	public void getCASSANDRAHealthCheck() {
		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);

		Request reqObj = new Request();
		reqObj.setOperation(ActorOperations.CASSANDRA.getValue());
		subject.tell(reqObj, probe.getRef());
		Response res = probe.expectMsgClass(duration("200 second"), Response.class);
		Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
	}

}
