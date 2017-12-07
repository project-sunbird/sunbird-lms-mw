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
import org.sunbird.learner.util.Util;

/**
 * 
 * @author Amit Kumar
 *
 */
public class UpdateUserCountScheduler implements Job {

  @Override
  public void execute(JobExecutionContext ctx) throws JobExecutionException {
    ProjectLogger.log("Running Update user count Scheduler Job at: " + Calendar.getInstance().getTime()
        + " triggered by: " + ctx.getJobDetail().toString(), LoggerEnum.INFO.name());
    List<Object> locIdList = new ArrayList<>();
    Util.DbInfo geoLocationDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
    CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    Map<String,Object> locMap = new HashMap<>();
    locMap.put(JsonKey.USER_COUNT, null);
    ProjectLogger.log("fetching data from cassandra where userCount is NULL.");
    Response response = cassandraOperation.getRecordsByProperties(geoLocationDbInfo.getKeySpace(), geoLocationDbInfo.getTableName(), locMap);
    List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    ProjectLogger.log("size of data from cassandra where userCount is NULL. = "+list.size());
    locMap.put(JsonKey.USER_COUNT, 0);
    ProjectLogger.log("fetching data from cassandra where userCount is ZERO.");
    Response response2 = cassandraOperation.getRecordsByProperties(geoLocationDbInfo.getKeySpace(), geoLocationDbInfo.getTableName(), locMap);
    ProjectLogger.log("size of data from cassandra where userCount is NULL. = "+((List<Map<String, Object>>) response2.get(JsonKey.RESPONSE)).size());
    list.addAll((List<Map<String, Object>>) response2.get(JsonKey.RESPONSE));
    for(Map<String, Object> map : list){
      locIdList.add(map.get(JsonKey.ID));
    }
    ProjectLogger.log("size of total locId to processed = "+locIdList.size());
    Request request = new Request();
    request.setOperation(ActorOperations.UPDATE_USER_COUNT_TO_LOCATIONID.getValue());
    request.getRequest().put(JsonKey.LOCATION_IDS, locIdList);
    request.getRequest().put(JsonKey.OPERATION, "UpdateUserCountScheduler");
    ProjectLogger.log("calling BackgroundService actor from scheduler");
    ActorUtil.tell(request);
  }

}
