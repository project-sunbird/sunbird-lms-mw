package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author arvind
 */
public class UtilityActor extends UntypedAbstractActor {

    private CassandraOperation cassandraOperation = new CassandraOperationImpl();
    private LogHelper logger = LogHelper.getInstance(UtilityActor.class.getName());
    private final String CONTENT_STATE_INFO= "contentStateInfo";
    SimpleDateFormat sdf = ProjectUtil.format;

    @SuppressWarnings("unchecked")
	@Override
    public void onReceive(Object message) throws Throwable {
    	

        if (message instanceof Request) {
            Request req = (Request) message;

            // get the list of content objects
            List<Map<String, Object>> contentList = (List<Map<String, Object>>) req.getRequest().get(JsonKey.CONTENTS);
            // get the content state info
            Map<String, Integer> contentStateInfo = (Map<String, Integer>) req.get(this.CONTENT_STATE_INFO);

            Map<String, Object> temp = new HashMap<String, Object>();

            for (Map<String, Object> map : contentList) {

                String contentid = (String) map.get(JsonKey.ID);

                if (map.get(JsonKey.COURSE_ID) != null) {
                    String primary = (String)map.get(JsonKey.COURSE_ID);
                    if(temp.containsKey(primary)){
                        Map<String , Object> innerMap = (Map<String , Object>)temp.get(primary);
                        innerMap.put("content" , getLatestContent((Map<String, Object>) ((Map<String , Object>)temp.get(primary)).get("content"), map));
                        if(((int)map.get(JsonKey.COMPLETED_COUNT))==1 && contentStateInfo.get(contentid)==2){
                            innerMap.put("progress",(Integer)innerMap.get("progress")+1);
                        }

                    }else{
                        Map<String , Object> innerMap = new HashMap<String,Object>();
                        innerMap.put("content" , map);
                        if(((int)map.get(JsonKey.COMPLETED_COUNT))==1 && contentStateInfo.get(contentid)==2){
                            innerMap.put("progress",new Integer(1));
                        }else{
                            innerMap.put("progress",new Integer(0));
                        }
                        temp.put(primary ,  innerMap);
                    }
                }else{
                    // no need to update since content does not belong to any course
                }
            }
            //logic to update the course
            updateCourse(temp , contentStateInfo);

        }
    }

    /**
     * Method to update the course_enrollment with the latest content information
     * @param temp Map<String, Object>
     * @param contentStateInfo Map<String, Integer>
     */
    @SuppressWarnings("unchecked")
	private void updateCourse(Map<String, Object> temp, Map<String, Integer> contentStateInfo) {

        Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);

        for (Map.Entry<String, Object> entry : temp.entrySet())
        {
            String key = entry.getKey();
			Map<String , Object> value = (Map<String , Object>) entry.getValue();

            Response response = cassandraOperation.getRecordById(dbInfo.getKeySpace() , dbInfo.getTableName() , key);

            List<Map<String , Object>> courseList = (List<Map<String , Object>>)response.getResult().get(JsonKey.RESPONSE);
            Map<String , Object> course = null;
            if(null != courseList && courseList.size()>0){
              course = courseList.get(0);

              Integer courseProgress = 0;
              if(ProjectUtil.isNotNull(course.get(JsonKey.COURSE_PROGRESS))) {
                  courseProgress = (Integer) course.get(JsonKey.COURSE_PROGRESS);
              }
              courseProgress = courseProgress+(Integer)value.get("progress");

              course.put(JsonKey.COURSE_PROGRESS , courseProgress);
              course.put(JsonKey.LAST_READ_CONTENTID ,((Map<String,Object>)value.get("content")).get(JsonKey.CONTENT_ID));
              course.put(JsonKey.LAST_READ_CONTENT_STATUS , (contentStateInfo.get((String)((Map<String,Object>)value.get("content")).get(JsonKey.ID))));
               try {
                  cassandraOperation.upsertRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), course);
              }catch(Exception ex){
                  logger.error(ex);
              }
            }
            
        }

    }

    private boolean activeStatus(Object obj) {

        if(null == obj || !(obj instanceof Boolean)){
            return false;
        } else{
            return (boolean)obj;
        }


    }

    private Map<String , Object> getLatestContent(Map<String , Object> current , Map<String , Object> next){
        if(current.get(JsonKey.LAST_ACCESS_TIME) == null && next.get(JsonKey.LAST_ACCESS_TIME) == null){
            return next;
        }else if(current.get(JsonKey.LAST_ACCESS_TIME) == null){
            return next;
        }else if(next.get(JsonKey.LAST_ACCESS_TIME) == null){
            return current;
        }
        try {
            Date currentUpdatedTime = sdf.parse((String)current.get(JsonKey.LAST_ACCESS_TIME));
            Date nextUpdatedTime = sdf.parse((String)next.get(JsonKey.LAST_ACCESS_TIME));
            if(currentUpdatedTime.after(nextUpdatedTime)){
                return current ;
            }else{
                return next;
            }
        } catch (ParseException e) {
            logger.error(e);
        }
        return null;
    }
}
