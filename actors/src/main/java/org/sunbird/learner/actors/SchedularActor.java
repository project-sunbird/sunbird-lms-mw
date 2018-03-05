package org.sunbird.learner.actors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.ActorUtil;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;

import akka.actor.UntypedAbstractActor;


/**
 * 
 * @author Amit Kumar
 *
 */
public class SchedularActor extends UntypedAbstractActor {

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("SchedularActor onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.SCHEDULE_BULK_UPLOAD.getValue())) {
          schedule(actorMessage);
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
      }
    } else {
      ProjectLogger.log("UNSUPPORTED MESSAGE");
    }
  }

  @SuppressWarnings("unchecked")
  private void schedule(Request request) {
    List<Map<String, Object>> result = (List<Map<String, Object>>) request.get(JsonKey.DATA);
    Util.DbInfo bulkDb = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
    CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    for (Map<String, Object> map : result) {
      int retryCount = 0;
      if (null != map.get(JsonKey.RETRY_COUNT)) {
       retryCount = (int) map.get(JsonKey.RETRY_COUNT);
      }
      if (retryCount > 2) {
        String data = (String) map.get(JsonKey.DATA);
        try {
          Map<String, Object> bulkMap = new HashMap<>();
          bulkMap.put(JsonKey.DATA, UserUtility.encryptData(data));
          bulkMap.put(JsonKey.PROCESS_ID, map.get(JsonKey.ID));
          bulkMap.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.FAILED.getValue());
          cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), bulkMap);
        } catch (Exception e) {
          ProjectLogger.log(
              "Exception ocurred while encrypting data while running scheduler for bulk upload process : ",
              e);
        }
      } else {
        Map<String, Object> bulkMap = new HashMap<>();
        bulkMap.put(JsonKey.RETRY_COUNT, retryCount+1);
        bulkMap.put(JsonKey.PROCESS_ID, map.get(JsonKey.ID));
        bulkMap.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.IN_PROGRESS.getValue());
        cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), bulkMap);
        Request req = new Request();
        req.put(JsonKey.PROCESS_ID, map.get(JsonKey.ID));
        ProjectLogger.log("calling bulkUploadBackGroundJobActor for processId from schedular actor "
            + map.get(JsonKey.ID));
        req.setOperation(ActorOperations.PROCESS_BULK_UPLOAD.getValue());
        ActorUtil.tell(req);
      }
      }
  }

}
