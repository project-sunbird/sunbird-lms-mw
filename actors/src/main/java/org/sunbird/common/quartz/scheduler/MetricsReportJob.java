package org.sunbird.common.quartz.scheduler;

import akka.actor.ActorRef;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.ReportTrackingStatus;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.RequestRouterActor;
import org.sunbird.learner.util.ActorUtil;
import org.sunbird.learner.util.Util;

/**
 * Created by arvind on 30/8/17.
 */
public class MetricsReportJob implements Job {


  Util.DbInfo reportTrackingdbInfo = Util.dbInfoMap.get(JsonKey.REPORT_TRACKING_DB);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private int time = -30;

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    ProjectLogger.log("METRICS JOB TRIGGERED #############-----------");
    performReportJob();
  }

  private void performReportJob() {

    ObjectMapper mapper = new ObjectMapper();
    SimpleDateFormat simpleDateFormat = ProjectUtil.getDateFormatter();

    Response response = cassandraOperation.getRecordsByProperty(reportTrackingdbInfo.getKeySpace(),
        reportTrackingdbInfo.getTableName(), JsonKey.STATUS,
        ReportTrackingStatus.UPLOADING_FILE.getValue());

    List<Map<String, Object>> dbResult = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (!(dbResult.isEmpty())) {
      // TODO: perform logic here 1. the updated date value should be older than 30 minutes then
      // pick that one
      Calendar now = Calendar.getInstance();
      now.add(Calendar.MINUTE, time);
      Date thirtyMinutesBefore = now.getTime();
      for (Map<String, Object> map : dbResult) {
        String updatedDate = (String) map.get(JsonKey.UPDATED_DATE);
        try {
          if (thirtyMinutesBefore.compareTo(simpleDateFormat.parse(updatedDate)) >= 0) {
            String jsonString = (String) map.get(JsonKey.DATA);
            // convert that string to List<List<Object>>
            TypeReference<List<List<Object>>> typeReference =
                new TypeReference<List<List<Object>>>() {};
            List<List<Object>> data = mapper.readValue(jsonString, typeReference);
            // assign the back ground task to background job actor ...
            Request backGroundRequest = new Request();
            backGroundRequest.setOperation(ActorOperations.FILE_GENERATION_AND_UPLOAD.getValue());

            Map<String, Object> innerMap = new HashMap<>();
            innerMap.put(JsonKey.REQUEST_ID, map.get(JsonKey.ID));
            innerMap.put(JsonKey.DATA, data);

            backGroundRequest.setRequest(innerMap);
            ActorUtil.tell(backGroundRequest);
          }
        } catch (ParseException e) {
          ProjectLogger.log(e.getMessage(), e);
        } catch (JsonParseException e) {
          ProjectLogger.log(e.getMessage(), e);
        } catch (JsonMappingException e) {
          ProjectLogger.log(e.getMessage(), e);
        } catch (IOException e) {
          ProjectLogger.log(e.getMessage(), e);
        }
      }
    }

    response = cassandraOperation.getRecordsByProperty(reportTrackingdbInfo.getKeySpace(),
        reportTrackingdbInfo.getTableName(), JsonKey.STATUS,
        ReportTrackingStatus.SENDING_MAIL.getValue());

    dbResult = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (!(dbResult.isEmpty())) {
      // TODO: perform logic here 1. the updated value should be older than 30 minutes then pick
      // that one
      Calendar now = Calendar.getInstance();
      now.add(Calendar.MINUTE, time);
      Date thirtyMinutesBefore = now.getTime();
      for (Map<String, Object> map : dbResult) {
        String updatedDate = (String) map.get(JsonKey.UPDATED_DATE);

        try {
          if (thirtyMinutesBefore.compareTo(simpleDateFormat.parse(updatedDate)) >= 0) {

            Request backGroundRequest = new Request();
            backGroundRequest.setOperation(ActorOperations.SEND_MAIL.getValue());

            Map<String, Object> innerMap = new HashMap<>();
            innerMap.put(JsonKey.REQUEST_ID, map.get(JsonKey.ID));

            backGroundRequest.setRequest(innerMap);
            ActorUtil.tell(backGroundRequest);

          }

        } catch (ParseException e) {
          ProjectLogger.log(e.getMessage(), e);
        }

      }

    }
  }
}
