/**
 * 
 */
package org.sunbird.common.quartz.scheduler;

import akka.actor.ActorRef;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

/**
 * This class will lookup into bulk process table.
 * if process type is new or in progress (more than x hours) then
 * take the process id and do the re-process of job.
 * @author Manzarul
 *
 */
public class UploadLookUpScheduler implements Job {
  private SimpleDateFormat format= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ");
  private ActorRef bulkUploadBackGroundJobActorRef = null;

  public void execute(JobExecutionContext ctx) throws JobExecutionException {
    ProjectLogger.log("Running Upload Scheduler Job at: " + Calendar.getInstance().getTime() +
        " triggered by: " + ctx.getJobDetail().toString());
    Util.DbInfo  bulkDb = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
    CassandraOperation cassandraOperation = new CassandraOperationImpl();
    List<Map<String,Object>> result = null;
    //get List of InProgress process
    Response res = cassandraOperation.getRecordsByProperty(bulkDb.getKeySpace(), bulkDb.getTableName(),
                          JsonKey.STATUS, ProjectUtil.BulkProcessStatus.IN_PROGRESS.getValue());
    result = ((List<Map<String,Object>>)res.get(JsonKey.RESPONSE));
    process(result);
    //get List of new Process
    res = cassandraOperation.getRecordsByProperty(bulkDb.getKeySpace(), bulkDb.getTableName(),
                          JsonKey.STATUS, ProjectUtil.BulkProcessStatus.NEW.getValue());
    result = ((List<Map<String,Object>>)res.get(JsonKey.RESPONSE));
    if(null != result){
      Iterator<Map<String, Object>> itr = result.iterator();
      while(itr.hasNext()){
        Map<String,Object> map = itr.next();
        try{
          Date startTime =  format.parse((String) map.get(JsonKey.PROCESS_START_TIME));
          Date currentTime = format.parse(format.format(new Date()));
          long difference = currentTime.getTime() - startTime.getTime();
          int hourDiff = (int) (difference/(1000*3600));
          //if diff is more than 5Hr then only process it.
          if(hourDiff < 5){
            itr.remove();
          }
        }catch(Exception ex){
          ProjectLogger.log(ex.getMessage(), ex);
        }
      }
      process(result);
    }
  }

  private void process(List<Map<String, Object>> result) {
    for(Map<String,Object> map : result){
      Request request = new Request();
      request.put(JsonKey.PROCESS_ID, map.get(JsonKey.PROCESS_ID));
      request.setOperation(ActorOperations.PROCESS_BULK_UPLOAD.getValue());
      bulkUploadBackGroundJobActorRef.tell(request, null);
    }
  }
}
