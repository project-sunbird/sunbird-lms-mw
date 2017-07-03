/**
 *
 */
package org.sunbird.learner.actors;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;

import akka.actor.UntypedAbstractActor;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.duration.Duration;

/**
 * This actor will handle course enrollment operation .
 *
 * @author Manzarul
 */
public class CourseEnrollmentActor extends UntypedAbstractActor {
    private LogHelper logger = LogHelper.getInstance(CourseEnrollmentActor.class.getName());

     private CassandraOperation cassandraOperation = new CassandraOperationImpl();

    /**
     * Receives the actor message and perform the course enrollment operation .
     * @param message Object (Request)
     * @throws Throwable
     */
    @SuppressWarnings("unchecked")
	@Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof Request) {
            logger.info("CourseEnrollmentActor  onReceive called");
            Request actorMessage = (Request) message;
            if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.ENROLL_COURSE.getValue())) {
                    Util.DbInfo courseEnrollmentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
                    Util.DbInfo courseMgmtdbInfo=Util.dbInfoMap.get(JsonKey.COURSE_MANAGEMENT_DB);
                    Map<String , Object> req = actorMessage.getRequest();
                    String addedBy = (String) req.get(JsonKey.REQUESTED_BY);
                    Map<String , Object> courseMap=(Map<String, Object>) req.get(JsonKey.COURSE);
                    Response response = cassandraOperation.getPropertiesValueById(courseMgmtdbInfo.getKeySpace(),courseMgmtdbInfo.getTableName(),(String)courseMap.get(JsonKey.COURSE_ID),JsonKey.COURSE_LOGO_URL,JsonKey.CONTENT_ID);
                    List<Map<String , Object>> courseMgmtList = (List<Map<String , Object>>)response.get(JsonKey.RESPONSE);
                    Map<String , Object> courseMgmtObject = null;
                    if(courseMgmtList!=null && courseMgmtList.size() > 0){
                    	courseMgmtObject = courseMgmtList.get(0);
                    	courseMap.put(JsonKey.COURSE_LOGO_URL, courseMgmtObject.get(JsonKey.COURSE_LOGO_URL));
                    	courseMap.put(JsonKey.CONTENT_ID, courseMgmtObject.get(JsonKey.CONTENT_ID));
                    }
                    courseMap.put(JsonKey.ADDED_BY , addedBy);
                    courseMap.put(JsonKey.COURSE_ENROLL_DATE ,ProjectUtil.getFormattedDate());
                    courseMap.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
                    courseMap.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
                    courseMap.put(JsonKey.DATE_TIME , new Timestamp(new Date().getTime()));
                    courseMap.put(JsonKey.ID ,generateandAppendPrimaryKey(courseMap));
                    courseMap.put(JsonKey.COURSE_PROGRESS , 0);
                    Response result = cassandraOperation.insertRecord(courseEnrollmentdbInfo.getKeySpace(),courseEnrollmentdbInfo.getTableName(),courseMap);
                    sender().tell(result, self());
                    //update user count in course mgmt table
                    updateCoursemanagement(ActorOperations.UPDATE_USER_COUNT.getValue(), courseMap.get(JsonKey.COURSE_ID), ActorOperations.UPDATE_USER_COUNT.getValue());
            } else {
                logger.info("UNSUPPORTED OPERATION");
                ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
                sender().tell(exception, self());
            }
        } else {
            // Throw exception as message body
            logger.info("UNSUPPORTED MESSAGE");
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
        }
    }
    
    /**
     * This method will combined map values with delimiter and create an encrypted key. 
     * @param req Map<String , Object>
     * @return String encrypted value
     */
    private String generateandAppendPrimaryKey(Map<String , Object> req){
        String userId = (String)req.get(JsonKey.USER_ID);
        String courseId = (String)req.get(JsonKey.COURSE_ID);
        return OneWayHashing.encryptVal(userId+ JsonKey.PRIMARY_KEY_DELIMETER+courseId);
    }
    
    /**
     * This method will call the background job manager and update course enroll user count.
     * @param ooperation String (operation name)
     * @param courseData Object 
     * @param innerOperation String
     */
    private void updateCoursemanagement(String ooperation, Object courseData,String innerOperation) {
    	 Timeout timeout = new Timeout(Duration.create(ProjectUtil.BACKGROUND_ACTOR_WAIT_TIME, TimeUnit.SECONDS));
		 Response userCountresponse = new Response();
		 userCountresponse.put(JsonKey.OPERATION, ooperation);
		 userCountresponse.put(JsonKey.COURSE_ID,courseData);
		 userCountresponse.getResult().put(JsonKey.OPERATION, innerOperation);
		 Patterns.ask(RequestRouterActor.backgroundJobManager, userCountresponse, timeout); 
    }
    
}
