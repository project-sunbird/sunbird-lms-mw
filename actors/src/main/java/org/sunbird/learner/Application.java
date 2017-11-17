package org.sunbird.learner;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.learner.actors.BackgroundRequestRouterActor;
import org.sunbird.learner.actors.RequestRouterActor;
import org.sunbird.learner.util.SchedulerManager;
import org.sunbird.learner.util.Util;

/**
 * @author Amit Kumar
 * @author arvind
 * 
 * Remote actor Application start point .
 */
public class Application {
  private static ActorSystem system;
  private static final String ACTOR_CONFIG_NAME = "RemoteMWConfig";
  private static final String ACTOR_LOCAL_CONFIG_NAME = "LocaleMWConfig";
  private static final String LOCAL_ACTOR_SYSTEM_NAME = "LocalMiddlewareActorSystem";
  private static final String REMOTE_ACTOR_SYSTEM_NAME = "RemoteMiddlewareActorSystem";
  private static final String BKG_ACTOR_CONFIG_NAME = "BackgroundRemoteMWConfig";
  private static final String BKG_ACTOR_LOCAL_CONFIG_NAME = "BackGroundLocalMWConfig";
  private static final String BKG_LOCAL_ACTOR_SYSTEM_NAME = "BackGroundLocalMiddlewareActorSystem";
  private static final String BKG_REMOTE_ACTOR_SYSTEM_NAME =
      "BackGroundRemoteMiddlewareActorSystem";
  static PropertiesCache cache = PropertiesCache.getInstance();

  public static void main(String[] args) {
    /*
     * System.getenv(JsonKey.ACTOR_SERVICE_INSTANCE) i.e "actor_service_name" values are
     * ("RemoteMiddlewareActorSystem" ,"BackGroundRemoteMiddlewareActorSystem").
     *  
     * based on value of
     * "actor_service_name" env variable , this application will start the actor system on a machine
     * i.e if value is "RemoteMiddlewareActorSystem" it will start Normal Actor System on that machine
     * and if value is "BackGroundRemoteMiddlewareActorSystem" , it will start background actor
     * system on that machine
     */
    String actorSystemToStart = System.getenv(JsonKey.ACTOR_SERVICE_INSTANCE);
    /*
     * Temporary changes for running learning service and actor service only on 2 machine instead of 3 machine
     * 
     * if have to run normal actor and background actor both remotely then change the value of this variable 
     * accordingly on machine and read from environment variable and make the value of this variable in config file i.e in
     * externalResource properties file to NULL or empty.
     * 
     * actorSystemToStart : reading this value from properties file to start any actor system as remote
     * only one actor system , can run remotely at a time with this change (other actor system should run locally)
     * if you want to run background actor remotely then change the value of actorSystemToStart to BackGroundRemoteMiddlewareActorSystem
     * OR if you want to run normal actor remotely then change the value of actorSystemToStart to RemoteMiddlewareActorSystem
     */
    if(!ProjectUtil.isStringNullOREmpty(PropertiesCache.getInstance().getProperty(JsonKey.ACTOR_SERVICE_INSTANCE))){
      actorSystemToStart = PropertiesCache.getInstance().getProperty(JsonKey.ACTOR_SERVICE_INSTANCE);
    }
    if ("remote".equalsIgnoreCase(cache.getProperty("api_actor_provider"))
        && (REMOTE_ACTOR_SYSTEM_NAME.equalsIgnoreCase(actorSystemToStart))) {
      ProjectLogger.log("Initializing Normal Actor System remotely");
      startRemoteActorSystem();
    }
    if ("remote".equalsIgnoreCase(cache.getProperty("background_actor_provider"))
        && (BKG_REMOTE_ACTOR_SYSTEM_NAME.equalsIgnoreCase(actorSystemToStart))) {
      ProjectLogger.log("Initializing Background Actor System remotely ");
      startBackgroundRemoteActorSystem();
    } else if("local".equalsIgnoreCase(cache.getProperty("background_actor_provider"))){
      ProjectLogger.log("Initializing Background Actor System locally ");
      startBackgroundLocalActorSystem();
    }

  }

  /**
   * This method will do the basic setup for actors.
   */
  private static void startRemoteActorSystem() {
    ProjectLogger.log("startRemoteCreationSystem method called....");
    Config con = null;
    String host = System.getenv(JsonKey.SUNBIRD_ACTOR_SERVICE_IP);
    String port = System.getenv(JsonKey.SUNBIRD_ACTOR_SERVICE_PORT);

    if (!ProjectUtil.isStringNullOREmpty(host) && !ProjectUtil.isStringNullOREmpty(port)) {
      con = ConfigFactory
          .parseString(
              "akka.remote.netty.tcp.hostname=" + host + ",akka.remote.netty.tcp.port=" + port + "")
          .withFallback(ConfigFactory.load().getConfig(ACTOR_CONFIG_NAME));
    } else {
      con = ConfigFactory.load().getConfig(ACTOR_CONFIG_NAME);
    }
    system = ActorSystem.create(REMOTE_ACTOR_SYSTEM_NAME, con);
    ActorRef learnerActorSelectorRef = system.actorOf(Props.create(RequestRouterActor.class),
        RequestRouterActor.class.getSimpleName());

    RequestRouterActor.setSystem(system);

    ProjectLogger.log("normal remote ActorSelectorRef " + learnerActorSelectorRef);
    ProjectLogger.log("NORMAL ACTOR REMOTE SYSTEM STARTED " + learnerActorSelectorRef,
        LoggerEnum.INFO.name());
    checkCassandraConnection();
  }


  public static void checkCassandraConnection() {
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD_PLUGIN);
    SchedulerManager.schedule();
  }

  public static ActorRef startLocalActorSystem() {
    system = ActorSystem.create(LOCAL_ACTOR_SYSTEM_NAME,
        ConfigFactory.load().getConfig(ACTOR_LOCAL_CONFIG_NAME));
    ActorRef learnerActorSelectorRef = system.actorOf(Props.create(RequestRouterActor.class),
        RequestRouterActor.class.getSimpleName());
    ProjectLogger.log("normal local ActorSelectorRef " + learnerActorSelectorRef);
    ProjectLogger.log("NORNAL ACTOR LOCAL SYSTEM STARTED " + learnerActorSelectorRef,
        LoggerEnum.INFO.name());
    checkCassandraConnection();
    PropertiesCache cache = PropertiesCache.getInstance();
    if ("local".equalsIgnoreCase(cache.getProperty("background_actor_provider"))) {
      ProjectLogger.log("Initializing Local Background Actor System");
      startBackgroundLocalActorSystem();
    }
    return learnerActorSelectorRef;
  }

  /**
   * This method will do the basic setup for actors.
   */
  private static void startBackgroundRemoteActorSystem() {
    Config con = null;
    String host = System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_IP);
    String port = System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_PORT);

    if (!ProjectUtil.isStringNullOREmpty(host) && !ProjectUtil.isStringNullOREmpty(port)) {
      con = ConfigFactory
          .parseString(
              "akka.remote.netty.tcp.hostname=" + host + ",akka.remote.netty.tcp.port=" + port + "")
          .withFallback(ConfigFactory.load().getConfig(BKG_ACTOR_CONFIG_NAME));
    } else {
      con = ConfigFactory.load().getConfig(BKG_ACTOR_CONFIG_NAME);
    }
    ActorSystem system = ActorSystem.create(BKG_REMOTE_ACTOR_SYSTEM_NAME, con);
    ActorRef learnerActorSelectorRef =
        system.actorOf(Props.create(BackgroundRequestRouterActor.class),
            BackgroundRequestRouterActor.class.getSimpleName());
    ProjectLogger.log("start BkgRemoteCreationSystem method called....");
    ProjectLogger.log("bkgActorSelectorRef " + learnerActorSelectorRef);
    ProjectLogger.log("BACKGROUND ACTORS STARTED " + learnerActorSelectorRef,
        LoggerEnum.INFO.name());
    checkCassandraConnection();
  }

  public static ActorRef startBackgroundLocalActorSystem() {
    ActorSystem system = ActorSystem.create(BKG_LOCAL_ACTOR_SYSTEM_NAME,
        ConfigFactory.load().getConfig(BKG_ACTOR_LOCAL_CONFIG_NAME));
    ActorRef learnerActorSelectorRef =
        system.actorOf(Props.create(BackgroundRequestRouterActor.class),
            BackgroundRequestRouterActor.class.getSimpleName());
    ProjectLogger.log("BACKGROUND ACTOR LOCAL SYSTEM STARTED " + learnerActorSelectorRef,
        LoggerEnum.INFO.name());
    checkCassandraConnection();
    return learnerActorSelectorRef;
  }

}
