package org.sunbird.common.quartz.scheduler;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.ActorUtil;
import org.sunbird.learner.util.TelemetryUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.telemetry.util.lmaxdisruptor.TelemetryEvents;

/**
 * 
 * @author Amit Kumar
 *
 */
public class UpdateUserCountScheduler implements Job {


  @Override
  public void execute(JobExecutionContext ctx) throws JobExecutionException {
    ProjectLogger.log("Running Update user count Scheduler Job at: "
        + Calendar.getInstance().getTime() + " triggered by: " + ctx.getJobDetail().toString(),
        LoggerEnum.INFO.name());
    Util.initializeContextForSchedulerJob(JsonKey.SYSTEM, ctx.getFireInstanceId(), JsonKey.SCHEDULER_JOB);
    Map<String, Object> logInfo =
        genarateLogInfo(JsonKey.SYSTEM, ctx.getJobDetail().getDescription());
    List<Object> locIdList = new ArrayList<>();
    Util.DbInfo geoLocationDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
    CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    Response response = cassandraOperation.getAllRecords(geoLocationDbInfo.getKeySpace(),
        geoLocationDbInfo.getTableName());
    List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    for (Map<String, Object> map : list) {
      if (null == map.get(JsonKey.USER_COUNT) || 0 == ((int) map.get(JsonKey.USER_COUNT))) {
        locIdList.add(map.get(JsonKey.ID));
      }
    }
    ProjectLogger.log("size of total locId to processed = " + locIdList.size());
    Request request = new Request();
    request.setOperation(ActorOperations.UPDATE_USER_COUNT_TO_LOCATIONID.getValue());
    request.getRequest().put(JsonKey.LOCATION_IDS, locIdList);
    request.getRequest().put(JsonKey.OPERATION, "UpdateUserCountScheduler");
    ProjectLogger.log("calling BackgroundService actor from scheduler");
    ActorUtil.tell(request);
    TelemetryUtil.telemetryProcessingCall(logInfo, null, null, TelemetryEvents.LOG.getName());
  }


  private Map<String, Object> genarateLogInfo(String logType, String message) {

    Map<String, Object> info = new HashMap<>();
    info.put(JsonKey.LOG_TYPE, logType);
    long startTime = System.currentTimeMillis();
    info.put(JsonKey.START_TIME, startTime);
    info.put(JsonKey.MESSAGE, message);
    info.put(JsonKey.LOG_LEVEL, JsonKey.INFO);

    return info;
  }

}
