package org.sunbird.learner.util.actorutility;

import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.learner.util.actorutility.impl.LocalActorSystem;
import org.sunbird.learner.util.actorutility.impl.RemoteActorSystem;

/**
 * 
 * @author Amit Kumar
 *
 */
public final class ActorSystemFactory {

  private static ActorSystem actorSystem = null;

  private ActorSystemFactory() {}

  static {
    try{
    PropertiesCache cache = PropertiesCache.getInstance();
    if ("remote"
        .equalsIgnoreCase(cache.getProperty("background_actor_provider"))) {
      ProjectLogger.log("Initializing Remote Actor System in org.sunbird.learner.util.actorutility.ActorSystemFactory");
      createRemoteActorSystem();
    } else {
      ProjectLogger.log("Initializing Local Actor System in org.sunbird.learner.util.actorutility.ActorSystemFactory");
      createLocalActorSystem();
    }
    }catch(Exception ex){
      ProjectLogger.log("Exception In org.sunbird.learner.util.actorutility.ActorSystemFactory "+ex);
    }
  }
  
  /**
   * This method will initialize the local actor system.
   */
  private static void createLocalActorSystem () {
    ProjectLogger.log("Initializing Local Actor System");
    if (null == actorSystem) {
      actorSystem = LocalActorSystem.getInstance();
    }
  }
  
  /**
   * This method will initialize the remote actor system.
   */
  public static void createRemoteActorSystem() {
    if (null == actorSystem) {
      actorSystem = RemoteActorSystem.getInstance();
    }
  }

  public static ActorSystem getActorSystem() {
    return actorSystem;
  }
}
