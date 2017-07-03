package org.sunbird.learner;

import java.io.File;

import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.learner.actors.RequestRouterActor;
import org.sunbird.learner.util.SchedulerManager;
import org.sunbird.learner.util.Util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

/**
 * @author arvind.
 * Remote actor Application start point .
 */
public class Application {
    private static LogHelper logger = LogHelper.getInstance(Application.class.getName());
    private static ActorSystem system;
    private static final String ACTOR_CONFIG_NAME = "RemoteMWConfig";
    private static final String ACTOR_SYSTEM_NAME = "RemoteMiddlewareSystem";
    public static void main(String[] args) {
        startRemoteCreationSystem();
    }
   /**
    * This method will do the basic setup for actors.
    */
    private static void startRemoteCreationSystem(){
    	String filePath = System.getenv(JsonKey.SUNBIRD_ACTOR_FILE_PATH);
    	Config con = null;
    	File file = null;
    	if(!ProjectUtil.isStringNullOREmpty(filePath)){
    	   file = new File(System.getenv("sunbird_file_path"));
    	}
    	if( file !=null && file.exists()){
    	 con = ConfigFactory.parseFile(file).getConfig(ACTOR_CONFIG_NAME);
    	} else {
    	  con	= ConfigFactory.load().getConfig(ACTOR_CONFIG_NAME);
    	}
    	 system = ActorSystem.create(ACTOR_SYSTEM_NAME, con);
        ActorRef learnerActorSelectorRef = system.actorOf(Props.create(RequestRouterActor.class),
                RequestRouterActor.class.getSimpleName());
        logger.info("ACTORS STARTED " + learnerActorSelectorRef);
        checkCassandraConnection();
    }

    private static void checkCassandraConnection() {
        Util.checkCassandraDbConnections();
        SchedulerManager.schedule();
    }
}
