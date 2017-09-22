package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import java.util.List;
import java.util.Map;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.bulkupload.BulkUploadBackGroundJobActor;


/**
 * 
 * @author Amit Kumar
 *
 */
public class SchedularActor extends UntypedAbstractActor {

  private ActorRef bulkUploadBackGroundJobActor;
  public SchedularActor() {
    bulkUploadBackGroundJobActor = getContext().actorOf(Props.create(BulkUploadBackGroundJobActor.class), "bulkUploadBackGroundJobActor");
   }
  
  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("BulkUploadBackGroundJobActor onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.SCHEDULE_BULK_UPLOAD.getValue())) {
          schedule(actorMessage);
        }else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
      }
    }else {
      ProjectLogger.log("UNSUPPORTED MESSAGE");
    }
  }

  @SuppressWarnings("unchecked")
  private void schedule(Request request) {
    List<Map<String, Object>> result = (List<Map<String, Object>>) request.get(JsonKey.DATA);
    for(Map<String,Object> map : result){
      Request req = new Request();
      req.put(JsonKey.PROCESS_ID, map.get(JsonKey.ID));
      ProjectLogger.log("calling bulkUploadBackGroundJobActor for processId from schedular actor "+ map.get(JsonKey.ID));
      req.setOperation(ActorOperations.PROCESS_BULK_UPLOAD.getValue());
      bulkUploadBackGroundJobActor.tell(req, null);
    }
  }

}
