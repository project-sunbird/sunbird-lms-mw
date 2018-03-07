package org.sunbird.learner;

import java.lang.reflect.Constructor;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.learner.util.actorutility.ActorSystemFactory;
import org.sunbird.learner.util.actorutility.impl.RemoteActorSystem;

public class RemoteActorSystemTest {

	private static Class t = null;

	@BeforeClass
	public static void setUp() {
		try {
			t = Class.forName("org.sunbird.learner.Application");
		} catch (ClassNotFoundException e) {
			ProjectLogger.log(e.getMessage(), e);
		}
	}

	@Test
	public void instanceCreationTest() {
		Exception exp = null;
		try {
			Constructor<Application> constructor = t.getDeclaredConstructor();
			constructor.setAccessible(true);
			Application application = constructor.newInstance();
			Assert.assertNotNull(application);
		} catch (Exception e) {
			exp = e;
		}
		Assert.assertNull(exp);
	}

	@Test
	public void remoteActorTest() {
		Exception exp = null;
		try {
			t.getDeclaredMethod("startRemoteActorSystem");
		} catch (Exception e) {
			exp = e;
		}
		Assert.assertNull(exp);
	}

	// @Test
	public void remoteActorTest1() {
		ActorSystemFactory.createRemoteActorSystem();
		Object obj = ActorSystemFactory.getActorSystem();
		Assert.assertTrue(obj instanceof RemoteActorSystem);
	}

}
