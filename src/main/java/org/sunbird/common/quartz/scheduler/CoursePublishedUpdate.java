/**
 * 
 */
package org.sunbird.common.quartz.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.CourseBatchSchedulerUtil;
import org.sunbird.learner.util.Util;

/**
 * This class will call the EKstep get content api to 
 * know the status of course published. 
 * once course status becomes live then it will
 * update status under course published table and collect 
 * all participant  from course-batch table and register all those 
 * participant under user_course table and push the data to ES.
 * @author Manzarul
 *
 */
public class CoursePublishedUpdate implements Job {
  
  private static  Util.DbInfo coursePublishDBInfo = Util.dbInfoMap.get(JsonKey.COURSE_PUBLISHED_STATUS);
  
  private static String requestData = "{\"request\":{\"filters\":{\"identifier\":dataVal},\"fields\":[\"status\"]}}";
  public void execute(JobExecutionContext ctx) throws JobExecutionException {
    System.out.println("Running Course published Scheduler Job at: " + Calendar.getInstance().getTime() + " triggered by: " + ctx.getJobDetail().toString());
  }
  
  
  /**
   * This method will provide list of all those course ids , 
   * which we did the published but haven't got the status 
   * back as live.
   * @return List<String>
   */
  private static List<String> getAllUnPublishedCourseStatusId() {
    ProjectLogger.log("start of calling get unpublished course status==");
    List<String> ids = new ArrayList<String>();
    Response response = ServiceFactory.getInstance().getRecordsByProperty(
        coursePublishDBInfo.getKeySpace(), coursePublishDBInfo.getTableName(),
        JsonKey.STATUS, ProjectUtil.CourseMgmtStatus.DRAFT.ordinal());
    if (response != null && response.get(JsonKey.RESPONSE) != null) {
      Object obj = response.get(JsonKey.RESPONSE);
      if (obj != null && obj instanceof List) {
        List<Map<String, Object>> listOfMap = (List<Map<String, Object>>) obj;
        if (listOfMap != null) {
          for (Map<String, Object> map : listOfMap) {
            ids.add((String) map.get(JsonKey.ID));
          }
        }
      }
    }
    ProjectLogger.log("end of calling get unpublished course status==" + ids);
    return ids;
  }
  
  /**
   * This method will verify call the EKStep to know the course 
   * published status 
   * @param ids List<String>
   * @return  List<String>
   */
  private static List<String> getAllPublishedCourseListFromEKStep(
      List<String> ids) {
    List<String> liveCourseIds = new ArrayList<>();
    StringBuilder identifier = new StringBuilder("[ ");
    for (int i = 0; i < ids.size(); i++) {
      if (i == 0) {
        identifier.append(" \"" + ids.get(i) + "\"");
      } else {
        identifier.append(" ,\"" + ids.get(i) + "\"");
      }
    }
    identifier.append(" ] ");
    try {
      String response = HttpUtil.sendPostRequest("URL",
          requestData.replace("dataval", identifier.toString()),
          CourseBatchSchedulerUtil.headerMap);
      JSONObject object = new JSONObject(response);
      JSONObject resultObj = object.getJSONObject(JsonKey.RESULT);
      JSONArray array = resultObj.getJSONArray(JsonKey.CONTENT);
      for (int i = 0; i < array.length(); i++) {
        JSONObject innerObject = array.getJSONObject(i);
        String status = innerObject.getString(JsonKey.STATUS);
        if (ProjectUtil.CourseMgmtStatus.LIVE.getValue()
            .equalsIgnoreCase(status)) {
          liveCourseIds.add(innerObject.getString(JsonKey.IDENTIFIER));
        }
      }
    } catch (IOException | JSONException e) {
      e.printStackTrace();
    }
    return liveCourseIds;
  }
  
   /**
    * This method will update course published status into cassandra and 
    * then take the participant map from course batch table and the add those
    * user under user_course table plus add data inside ES. 
    * @param publishedIds
    * @return
    */
  private boolean updateCoursePublishedStatus (List<String> publishedIds) {
    
    
    return false;
  }
  
}
