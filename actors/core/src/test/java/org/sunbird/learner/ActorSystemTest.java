package org.sunbird.learner;

import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.learner.util.actorutility.ActorSystemFactory;
import org.sunbird.learner.util.actorutility.impl.LocalActorSystem;
import org.sunbird.learner.util.actorutility.impl.RemoteActorSystem;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import junit.framework.Assert;

public class ActorSystemTest {

	private static String provider = null;

	@BeforeClass
	public static void setUp() {
		Application.startLocalActorSystem();
		provider = PropertiesCache.getInstance().getProperty("background_actor_provider");
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testActorSystem() {
		Object obj = ActorSystemFactory.getActorSystem();
		if (provider.equalsIgnoreCase("local")) {
			Assert.assertTrue(obj instanceof LocalActorSystem);
		} else {
			Assert.assertTrue(obj instanceof RemoteActorSystem);
		}
	}

	@SuppressWarnings("deprecation")
	// @Test
	public void testActorRef() {
		Object obj = ActorSystemFactory.getActorSystem().initializeActorSystem(ActorOperations.CREATE_USER.getValue());
		if (provider.equalsIgnoreCase("local")) {
			Assert.assertTrue(obj instanceof ActorRef);
		} else {
			Assert.assertTrue(obj instanceof ActorSelection);
		}
	}

}
