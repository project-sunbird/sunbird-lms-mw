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
public class ActorSystemFactory {
  
  private static RemoteActorSystem remoteActorSystem = null;
  private static LocalActorSystem localActorSystem = null;
  
  private ActorSystemFactory(){}
  
  public static ActorSystem getActorSystem(){
    PropertiesCache cache = PropertiesCache.getInstance();
    if("remote".equalsIgnoreCase(cache.getProperty("background_actor_provider"))){
      ProjectLogger.log("Initializing Remote Actor System");
      if(null == remoteActorSystem){
        remoteActorSystem = new RemoteActorSystem();
        return remoteActorSystem ;
      }else{
        return remoteActorSystem;
      }
    }else{
      ProjectLogger.log("Initializing Local Actor System");
      if(null == localActorSystem){
        localActorSystem = new LocalActorSystem();
        return localActorSystem ;
      }else{
        return localActorSystem;
      }
    }
  }
}
