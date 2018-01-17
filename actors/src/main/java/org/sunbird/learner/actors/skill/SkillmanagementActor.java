package org.sunbird.learner.actors.skill;

import static org.sunbird.learner.util.Util.isNotNull;
import static org.sunbird.learner.util.Util.isNull;

import akka.actor.UntypedAbstractActor;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.telemetry.util.lmaxdisruptor.LMAXWriter;

/**
 * Class to provide functionality for Add and Endorse the user skills .
 * Created by arvind on 18/10/17.
 */
public class SkillmanagementActor extends UntypedAbstractActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo userSkillDbInfo = Util.dbInfoMap.get(JsonKey.USER_SKILL_DB);
  private Util.DbInfo skillsListDbInfo = Util.dbInfoMap.get(JsonKey.SKILLS_LIST_DB);
  Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
  private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
  private final String REF_SKILLS_DB_ID = "001";
  private LMAXWriter lmaxWriter = LMAXWriter.getInstance();

  @Override
  public void onReceive(Object message) throws Throwable {

    if (message instanceof Request) {

      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      try {
        ProjectLogger.log("SkillmanagementActor-onReceive called");
        Request actorMessage = (Request) message;
        initializeContext(actorMessage, JsonKey.USER, "USER_ENV");
        //set request id fto thread loacl...
        ExecutionContext.setRequestId(actorMessage.getRequestId());
        if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.ADD_SKILL.getValue())) {
          endorseSkill(actorMessage);

          //testDisruptor();
        } else if (actorMessage.getOperation()
              .equalsIgnoreCase(ActorOperations.GET_SKILL.getValue())) {
          getSkill(actorMessage);
        }else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_SKILLS_LIST.getValue())) {
          getSkillsList();
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION", LoggerEnum.INFO.name());
          ProjectCommonException exception =
              new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                  ResponseCode.invalidOperationName.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
        sender().tell(ex, self());
      }finally{
        // clean up the request level context info...
        //ExecutionContext.getCurrent().cleanup();
      }
    } else {
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  private void testDisruptor() {

    System.out.println("TESTING LMAX DISRUPTOR");
    //lmaxWriter.submitMessage("hello");
  }

  private void initializeContext(Request actorMessage, String actorType, String env) {

    ExecutionContext context = ExecutionContext.getCurrent();
    Map<String , Object> requestContext = new HashMap<>();
    // request level info ...
    Map<String , Object> req = actorMessage.getRequest();
    String requestedby = (String) req.get(JsonKey.REQUESTED_BY);
    String channel = (String) req.get(JsonKey.CHANNEL);
    String mid = (String) req.get(JsonKey.REQUEST_ID);
    requestContext.put(JsonKey.REQUEST_ID ,(String) req.get(JsonKey.REQUEST_ID));
    requestContext.put(JsonKey.CHANNEL, channel);
    requestContext.put("ACTOR_ID", requestedby);
    requestContext.put("ACTOR_TYPE" , actorType);
    requestContext.put("env",env);
    context.setRequestContext(requestContext);

    // and global context will be set at the time of creation of thread local automatically ...

  }

  /**
   * Method will return all the list of skills , it is type of reference data ...
   * @param actorMessage
   */
  private void getSkillsList() {

    ProjectLogger.log("SkillmanagementActor-getSkillsList called");
    Map<String, Object> skills = new HashMap<>();
    Response skilldbresponse=cassandraOperation.getRecordById(skillsListDbInfo.getKeySpace(), skillsListDbInfo.getTableName(), REF_SKILLS_DB_ID);
    List<Map<String, Object>> skillList =  (List<Map<String, Object>>) skilldbresponse.get(JsonKey.RESPONSE);

    if(! skillList.isEmpty()){
      skills = skillList.get(0);
    }
    Response response = new Response();
    response.getResult().put(JsonKey.SKILLS , skills.get(JsonKey.SKILLS));
    sender().tell(response, self());

  }

  /**
   * Method to get the list of skills of the user on basis of UserId ...
   * @param actorMessage
   */
  private void getSkill(Request actorMessage) {

    ProjectLogger.log("SkillmanagementActor-getSkill called");
    String endorsedUserId  = (String) actorMessage.getRequest().get(JsonKey.ENDORSED_USER_ID);
    if(ProjectUtil.isStringNullOREmpty(endorsedUserId)){
     throw new ProjectCommonException(ResponseCode.endorsedUserIdRequired.getErrorCode(),
          ResponseCode.endorsedUserIdRequired.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Map<String, Object> filter = new HashMap<>();
    filter.put(JsonKey.USER_ID, endorsedUserId);
    List<String> fields = new ArrayList<>();
    fields.add(JsonKey.SKILLS);

    Map<String, Object> result = ElasticSearchUtil.complexSearch(createESRequest(filter , null,
        fields), ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName());
    if(result.isEmpty() || ((List<Map<String , Object>>)result.get(JsonKey.CONTENT)).isEmpty()){
      throw new ProjectCommonException(ResponseCode.invalidUserId.getErrorCode(),
          ResponseCode.invalidUserId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    List<Map<String , Object>> skillList = (List<Map<String , Object>>)result.get(JsonKey.CONTENT);

    Map<String , Object> skillMap = new HashMap();
    if(! skillList.isEmpty()){
      skillMap = skillList.get(0);
    }

      Response response = new Response();
      response.getResult().put(JsonKey.SKILLS , skillMap.get(JsonKey.SKILLS));
      sender().tell(response , self());

  }

  /**
   * Method to add or endorse the user skill ...
   * @param actorMessage
   */
  private void endorseSkill(Request actorMessage) {

    ProjectLogger.log("SkillmanagementActor-endorseSkill called");
    format = new SimpleDateFormat("yyyy-MM-dd");
    // target object of telemetry event...
    Map<String, Object> targetObject = new HashMap<>();
    // correlated object of telemetry event...
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    String endoresedUserId  = (String) actorMessage.getRequest().get(JsonKey.ENDORSED_USER_ID);

    targetObject = generateTargetObject(endoresedUserId, JsonKey.USER);

    List<String> list = (List<String>) actorMessage.getRequest().get(JsonKey.SKILL_NAME);
    CopyOnWriteArraySet<String> skillset = new CopyOnWriteArraySet<>(list);
    String requestedByUserId = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

    Response response1=cassandraOperation.getRecordById(userDbInfo.getKeySpace(), userDbInfo.getTableName(), endoresedUserId);
    Response response2=cassandraOperation.getRecordById(userDbInfo.getKeySpace(), userDbInfo.getTableName(), requestedByUserId);
    List<Map<String, Object>> endoresedList =  (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);
    List<Map<String, Object>> requestedUserList =  (List<Map<String, Object>>) response2.get(JsonKey.RESPONSE);

    // check whether both userid exist or not if not throw exception
    if (endoresedList.isEmpty() || requestedUserList.isEmpty()) {
      //  generate context and params here ...
      Map<String, Object> context = getTelemetryContext();
      throw new ProjectCommonException(ResponseCode.invalidUserId.getErrorCode(),
          ResponseCode.invalidUserId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    Map<String, Object> endoresedMap = endoresedList.get(0);
    Map<String, Object> requestedUserMap = requestedUserList.get(0);

    // check whether both belongs to same org or not(check root  or id of both users)  , if not then throw exception ---
    if (!compareStrings((String) endoresedMap.get(JsonKey.ROOT_ORG_ID),
        (String) requestedUserMap.get(JsonKey.ROOT_ORG_ID))) {

      throw new ProjectCommonException(ResponseCode.canNotEndorse.getErrorCode(),
          ResponseCode.canNotEndorse.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    for(String skillName : skillset) {

      if (!ProjectUtil.isStringNullOREmpty(skillName)) {

        // check whether user have already this skill or not -
        String id = OneWayHashing
            .encryptVal(endoresedUserId + JsonKey.PRIMARY_KEY_DELIMETER + skillName.toLowerCase());
        Response response = cassandraOperation
            .getRecordById(userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(), id);
        List<Map<String, Object>> responseList = (List<Map<String, Object>>) response
            .get(JsonKey.RESPONSE);

        // prepare correlted object ...
        generateCorrelatedObject(id, "skill", "user.skill",correlatedObject);

        if (responseList.isEmpty()) {
          // means this is first time skill coming so add this one
          Map<String, Object> skillMap = new HashMap<>();
          skillMap.put(JsonKey.ID, id);
          skillMap.put(JsonKey.USER_ID, endoresedUserId);
          skillMap.put(JsonKey.SKILL_NAME, skillName);
          skillMap.put(JsonKey.SKILL_NAME_TO_LOWERCASE, skillName.toLowerCase());
          skillMap.put(JsonKey.ADDED_BY, requestedByUserId);
          skillMap.put(JsonKey.ADDED_AT, format.format(new Date()));
          Map<String, String> endoresers = new HashMap<>();

          List<Map<String , String>> endorsersList = new ArrayList<>();
          endoresers.put(JsonKey.USER_ID , requestedByUserId);
          endoresers.put(JsonKey.ENDORSE_DATE , format.format(new Date()));
          endorsersList.add(endoresers);

          skillMap.put(JsonKey.ENDORSERS_LIST, endorsersList);
          skillMap.put(JsonKey.ENDORSEMENT_COUNT, 0);
          cassandraOperation
              .insertRecord(userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(),
                  skillMap);

          updateEs(endoresedUserId);
        } else {
          // skill already exist for user simply update the then check if it is already added by same user then dont do anything
          // otherwise update the existing one ...

          Map<String, Object> responseMap = responseList.get(0);
          // check whether requested user has already endoresed to that user or not
          List<Map<String, String>> endoresersList = (List<Map<String, String>>) responseMap
              .get(JsonKey.ENDORSERS_LIST);
          boolean flag = false;
          for(Map<String , String> map : endoresersList){
            if(((String)map.get(JsonKey.USER_ID)).equalsIgnoreCase(requestedByUserId)){
              flag = true;
              break;
            }
          }
          if (flag) {
            // donot do anything..
            ProjectLogger.log(requestedByUserId + " has already endorsed the " + endoresedUserId);
          } else {
            Integer endoresementCount = (Integer) responseMap.get(JsonKey.ENDORSEMENT_COUNT) + 1;
            Map<String , String> endorsersMap = new HashMap<>();
            endorsersMap.put(JsonKey.USER_ID , requestedByUserId);
            endorsersMap.put(JsonKey.ENDORSE_DATE , format.format(new Date()));
            endoresersList.add(endorsersMap);

            responseMap.put(JsonKey.ENDORSERS_LIST, endoresersList);
            responseMap.put(JsonKey.ENDORSEMENT_COUNT, endoresementCount);
            cassandraOperation
                .updateRecord(userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(),
                    responseMap);
            updateEs(endoresedUserId);
          }
        }
      }else{
        skillset.remove(skillName);
      }
    }

    Response response3 = new Response();
    response3.getResult().put(JsonKey.RESULT, "SUCCESS");
    sender().tell(response3 , self());
    //TODO: group all information at one place and send it to background actor for processing , we have context full info, we have target obbject , correlated object info just pass all these to ackground actor it will take care of all these ...
    Request request = new Request();
    //request.setOperation("TELEMETRY_EVENT");

    Map<String, Object> params = new HashMap<>();
    // set additional props for edata related things ...
    params.put("props", actorMessage.getRequest().entrySet().stream().map(entry -> entry.getKey()).collect(
        Collectors.toList()));
    request.setRequest(genarateTelemetryRequest(targetObject , correlatedObject , "AUDIT", params));
    System.out.println("ACTOR SIDE TELEMETRY PROCESS STARTED ");
    lmaxWriter.submitMessage(request);
    //request.setOperation("TELEMETRY_EVENT");
    //ActorUtil.tell(request);
    // TODO: actor call
    updateSkillsList(skillset);
  }

  private Map<String,Object> getTelemetryContext() {

    Map<String, Object> context = new HashMap<>();
    context.putAll(ExecutionContext.getCurrent().getRequestContext());
    context.putAll(ExecutionContext.getCurrent().getGlobalContext());
    return context;
  }

  private Map<String, Object> generateTargetObject(String id, String type) {

    Map<String, Object> target = new HashMap<>();
    target.put(JsonKey.ID , id);
    target.put(JsonKey.TYPE , type);
    return target;
  }

  public void generateCorrelatedObject(String id, String type, String corelation, List<Map<String, Object>> correlationList){

    Map<String , Object> correlatedObject = new HashMap<String , Object>();
    correlatedObject.put(JsonKey.ID , id);
    correlatedObject.put(JsonKey.TYPE, type);
    correlatedObject.put(JsonKey.RELATION , corelation);

    correlationList.add(correlatedObject);

  }

  public Map<String, Object> genarateTelemetryRequest(Map<String, Object> targetObject,
      List<Map<String, Object>> correlatedObject, String eventType,
      Map<String, Object> params){

    Map<String, Object> map = new HashMap<>();
    map.put("TARGET_OBJECT", targetObject);
    map.put("CORRELATED_OBJECTS", correlatedObject);
    map.put("TELEMETRY_EVENT_TYPE", eventType);
    map.put("params", params);

    // combine context info into one i.e. request level and system level info into one place...

    Map<String, Object> context = getTelemetryContext();
    map.put("context", context);
    return map;


  }

  private void updateSkillsList(CopyOnWriteArraySet<String> skillset) {

    Map<String , Object> skills = new HashMap<>();
    List<String> skillsList = null;
    Response skilldbresponse=cassandraOperation.getRecordById(skillsListDbInfo.getKeySpace(), skillsListDbInfo.getTableName(), REF_SKILLS_DB_ID);
    List<Map<String, Object>> list =  (List<Map<String, Object>>) skilldbresponse.get(JsonKey.RESPONSE);

    if(! list.isEmpty()){
      skills = list.get(0);
      skillsList = (List<String>) skills.get(JsonKey.SKILLS);

    }else{
      // craete new Entry into the
      skillsList = new ArrayList<>();
    }

    for(String skillName : skillset){
      if(!skillsList.contains(skillName.toLowerCase())) {
        skillsList.add(skillName.toLowerCase());
      }
    }

      skills.put(JsonKey.ID , REF_SKILLS_DB_ID);
      skills.put(JsonKey.SKILLS , skillsList);
      cassandraOperation.upsertRecord(skillsListDbInfo.getKeySpace(), skillsListDbInfo.getTableName() ,skills);

  }

  @SuppressWarnings("unchecked")
  private void updateEs(String userId) {

    //  get all records from cassandra as list and add that list to user in ElasticSearch ...
    Response response = cassandraOperation.getRecordsByProperty(userSkillDbInfo.getKeySpace() , userSkillDbInfo.getTableName(), JsonKey.USER_ID , userId);
    List<Map<String,Object>> responseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    Map<String , Object> esMap = new HashMap<>();
    esMap.put(JsonKey.SKILLS , responseList);
    Map<String,Object> profile = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), userId);
    if(null!= profile && !profile.isEmpty()){
      Map<String,String> visibility = (Map<String, String>) profile.get(JsonKey.PROFILE_VISIBILITY);
      if((null!=visibility && !visibility.isEmpty())&& visibility.containsKey(JsonKey.SKILLS)){
        Map<String,Object> visibilityMap = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.userprofilevisibility.getTypeName(), userId);
        if (null != visibilityMap && !visibilityMap.isEmpty()) {
          visibilityMap.putAll(esMap);
          ElasticSearchUtil.createData(ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.userprofilevisibility.getTypeName(), userId , visibilityMap);
        }
      }else {
        ElasticSearchUtil.updateData(ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), userId , esMap);
      }
    } 
  }

  // method will compare two strings and return true id both are same otherwise false ...
  private boolean compareStrings(String first, String second) {
    if (isNull(first) && isNull(second)) {
      return true;
    }
    if ((isNull(first) && isNotNull(second)) || (isNull(second) && isNotNull(first))) {
      return false;
    }
    return first.equalsIgnoreCase(second);
  }

  protected SearchDTO createESRequest(Map<String, Object> filters, Map<String, String> aggs,
      List<String> fields) {
    SearchDTO searchDTO = new SearchDTO();

    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    if (ProjectUtil.isNotNull(aggs)) {
      searchDTO.getFacets().add(aggs);
    }
    if (ProjectUtil.isNotNull(fields)) {
      searchDTO.setFields(fields);
    }
    return searchDTO;
  }
}
