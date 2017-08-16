/**
 * 
 */
package org.sunbird.common.quartz.scheduler;

import akka.actor.ActorRef;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
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
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.learner.actors.RequestRouterActor;
import org.sunbird.learner.util.CourseBatchSchedulerUtil;
import org.sunbird.learner.util.EkStepRequestUtil;
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
  
  private ActorRef backGroundActorRef;

  public CoursePublishedUpdate() {
    backGroundActorRef = RequestRouterActor.backgroundJobManager;
   }
  
  private static  Util.DbInfo coursePublishDBInfo = Util.dbInfoMap.get(JsonKey.COURSE_PUBLISHED_STATUS);
  private Util.DbInfo courseBatchDBInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
  static CassandraOperation cassandraOperation = new CassandraOperationImpl();
  
  private static String requestData = "{\"request\":{\"filters\":{\"identifier\":dataVal},\"fields\":[\"status\"]}}";
  public void execute(JobExecutionContext ctx) throws JobExecutionException {
    System.out.println("Running Course published Scheduler Job at: " + Calendar.getInstance().getTime() + " triggered by: " + ctx.getJobDetail().toString());
    
    List<String> courseListWithStatusAsDraft = getAllUnPublishedCourseStatusId();
    if(null != courseListWithStatusAsDraft && !courseListWithStatusAsDraft.isEmpty()){
      List<String> ekStepResult = getAllPublishedCourseListFromEKStep(courseListWithStatusAsDraft);
      if(null != ekStepResult && !ekStepResult.isEmpty()){
        updateCourseStatusTable(ekStepResult);
        for(String courseId : ekStepResult){
          try{
            Map<String,Object> map = new HashMap<>();
            map.put(JsonKey.COURSE_ID, courseId);
            map.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
            Response response = cassandraOperation.getRecordsByProperty(courseBatchDBInfo.getKeySpace(), courseBatchDBInfo.getTableName(), JsonKey.COURSE_ID,courseId);
            List<Map<String,Object>> batchList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
            addUserToUserCourseTable(batchList);
          }catch(Exception ex){
            ProjectLogger.log(ex.getMessage(), ex);
          }
        }
      }
    }
  }
  
  
  private void addUserToUserCourseTable(List<Map<String, Object>> batchList) {
    Util.DbInfo courseEnrollmentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    for(Map<String,Object> batch : batchList){
      Map<String,String> additionalCourseInfo = (Map<String, String>) batch.get(JsonKey.COURSE_ADDITIONAL_INFO);
      if((int)batch.get(JsonKey.STATUS) != ProjectUtil.ProgressStatus.COMPLETED.getValue()){
        Map<String,Boolean> participants = (Map<String, Boolean>) batch.get(JsonKey.PARTICIPANT);
        for(Map.Entry<String,Boolean> entry : participants.entrySet()){
          if(!entry.getValue()){
            Map<String , Object> userCourses = new HashMap<>();
            userCourses.put(JsonKey.USER_ID , entry.getKey());
            userCourses.put(JsonKey.BATCH_ID , batch.get(JsonKey.ID));
            userCourses.put(JsonKey.COURSE_ID , batch.get(JsonKey.COURSE_ID));
            userCourses.put(JsonKey.ID , generatePrimaryKey(userCourses));
            userCourses.put(JsonKey.CONTENT_ID, batch.get(JsonKey.COURSE_ID));
            userCourses.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getFormattedDate());
            userCourses.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
            userCourses.put(JsonKey.STATUS, (int)batch.get(JsonKey.STATUS));
            userCourses.put(JsonKey.DATE_TIME, new Timestamp(new Date().getTime()));
            userCourses.put(JsonKey.COURSE_PROGRESS, 0);
            userCourses.put(JsonKey.COURSE_LOGO_URL, additionalCourseInfo.get(JsonKey.APP_ICON));
            userCourses.put(JsonKey.COURSE_NAME, additionalCourseInfo.get(JsonKey.NAME));
            userCourses.put(JsonKey.DESCRIPTION, additionalCourseInfo.get(JsonKey.DESCRIPTION));
            if(ProjectUtil.isStringNullOREmpty(additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT))){
              userCourses.put(JsonKey.LEAF_NODE_COUNT, additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT));
            }
            try {
              cassandraOperation
                  .insertRecord(courseEnrollmentdbInfo.getKeySpace(), courseEnrollmentdbInfo.getTableName(),
                      userCourses);
              insertUserCoursesToES(userCourses);
              //update participant map value as true
              entry.setValue(true);
            }catch(Exception ex) {
              ProjectLogger.log("INSERT RECORD TO USER COURSES EXCEPTION ",ex);
            }
          }
        }
        Map<String,Object> updatedBatch = new HashMap<>();
        updatedBatch.put(JsonKey.ID, batch.get(JsonKey.ID));
        updatedBatch.put(JsonKey.PARTICIPANT, participants);
        cassandraOperation.updateRecord(courseBatchDBInfo.getKeySpace(), courseBatchDBInfo.getTableName(), updatedBatch);
      }
    }
    
  }
  private String generatePrimaryKey(Map<String, Object> req) {
    String userId = (String) req.get(JsonKey.USER_ID);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    String batchId = (String) req.get(JsonKey.BATCH_ID);
    return OneWayHashing.encryptVal(userId + JsonKey.PRIMARY_KEY_DELIMETER + courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId);

  }

  private void updateCourseStatusTable(List<String> ekStepResult) {
    Map<String,Object> map = null;
    for(String courseId : ekStepResult){
      map = new HashMap<>();
      map.put(JsonKey.ID, courseId);
      map.put(JsonKey.STATUS, ProjectUtil.CourseMgmtStatus.LIVE.ordinal());
      try{
        cassandraOperation.updateRecord(coursePublishDBInfo.getKeySpace(), coursePublishDBInfo.getTableName(), map);
      }catch(Exception ex){
        ProjectLogger.log(ex.getMessage(), ex);
      }
    }
  }


  /**
   * This method will provide list of all those course ids , 
   * which we did the published but haven't got the status 
   * back as live.
   * @return List<String>
   */
  private List<String> getAllUnPublishedCourseStatusId() {
    ProjectLogger.log("start of calling get unpublished course status==");
    List<String> ids = new ArrayList<>();
    Response response = cassandraOperation.getRecordsByProperty(
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
  private List<String> getAllPublishedCourseListFromEKStep(
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
    Object[] result = EkStepRequestUtil.searchContent(requestData.replace("dataVal", identifier.toString()), 
          CourseBatchSchedulerUtil.headerMap);
      for (int i = 0; i < result.length; i++) {
        Map<String,Object> map = (Map<String, Object>) result[i];
        String status = (String) map.get(JsonKey.STATUS);
        if (ProjectUtil.CourseMgmtStatus.LIVE.getValue()
            .equalsIgnoreCase(status)) {
          liveCourseIds.add((String) map.get(JsonKey.IDENTIFIER));
        }
      }
    return liveCourseIds;
  }
  
  
  private void insertUserCoursesToES(Map<String, Object> courseMap) {
    Response response = new Response();
    response.put(JsonKey.OPERATION, ActorOperations.INSERT_USR_COURSES_INFO_ELASTIC.getValue());
    response.put(JsonKey.USER_COURSES, courseMap);
    try{
      backGroundActorRef.tell(response,null);
    }catch(Exception ex){
      ProjectLogger.log("Exception Occured during saving user course to Es : ", ex);
    }
  }
  
}
