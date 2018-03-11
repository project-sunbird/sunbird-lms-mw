package org.sunbird.actor.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.RouterMode;
import org.sunbird.actor.router.BackgroundRequestRouter;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.common.models.util.JsonKey;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.actor.Props;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class SunbirdMWService {

	private static String actorMode;
	private static ActorSystem system;
	private static String name = "SunbirdMWSystem";
	private static Config config;

	public static void init() {
		config = ConfigFactory.systemEnvironment().withFallback(ConfigFactory.load());
		getActorSystem();
		initRouters();
	}

	public static String getMode() {
		if (StringUtils.isBlank(actorMode)) {
			List<String> routers = Arrays.asList(RequestRouter.getMode(), BackgroundRequestRouter.getMode());
			long localCount = routers.stream().filter(mode -> StringUtils.equalsIgnoreCase(mode, "local")).count();
			actorMode = (routers.size() == localCount) ? "local" : "remote";
		}
		return actorMode;
	}

	public static ActorSystem getActorSystem() {
		if (null == system) {
			Config conf;
			if ("remote".equals(getMode())) {
				Config remote = getRemoteConfig();
				conf = remote.withFallback(config.getConfig(name));
			} else {
				conf = config.getConfig(name);
			}
			System.out.println("ActorSystem starting with mode: " + getMode());
			System.out.println("Config: " + conf);
			system = ActorSystem.create(name, conf);
		}
		return system;
	}

	private static Config getRemoteConfig() {
		List<String> details = new ArrayList<String>();
		details.add("akka.actor.provider=akka.remote.RemoteActorRefProvider");
		details.add("akka.remote.enabled-transports = [\"akka.remote.netty.tcp\"]");

		String host = System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_IP);
		String port = System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_PORT);
		if (StringUtils.isNotBlank(host))
			details.add("akka.remote.netty.tcp.hostname=" + host);
		if (StringUtils.isNotBlank(port))
			details.add("akka.remote.netty.tcp.port=" + port);
		return ConfigFactory.parseString(StringUtils.join(details, ","));
	}

	private static void initRouters() {
		System.out.println("RequestRouter.getMode(): " + RequestRouter.getMode());
		if (!RouterMode.OFF.name().equalsIgnoreCase(RequestRouter.getMode())) {
			system.actorOf(Props.create(RequestRouter.class), RequestRouter.class.getSimpleName());
		}
		System.out.println("BackgroundRequestRouter.getMode(): " + BackgroundRequestRouter.getMode());
		if (!RouterMode.OFF.name().equalsIgnoreCase(BackgroundRequestRouter.getMode())) {
			system.actorOf(Props.create(BackgroundRequestRouter.class), BackgroundRequestRouter.class.getSimpleName());
		}
	}

}
