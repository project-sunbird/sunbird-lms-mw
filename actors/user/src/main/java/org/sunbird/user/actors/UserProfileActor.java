package org.sunbird.user.actors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.SocialMediaType;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;


@ActorConfig(
    tasks = {"profileVisibility", "getMediaTypes"},
    asyncTasks = {})
public class UserProfileActor extends UserBaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    // set request id fto thread loacl...
    ExecutionContext.setRequestId(request.getRequestId());
    String operation = request.getOperation();
    switch (operation) {
      case "getMediaTypes":
        getMediaTypes();
        break;
      case "profileVisibility":
        profileVisibility(request);
        break;
      default:
        onReceiveUnsupportedMessage("UserProfileActor");
    }
  }

  private void getMediaTypes() {
    Response response = SocialMediaType.getMediaTypeFromDB();
    sender().tell(response, self());
  }

  /**
   * This method will first check user exist with us or not. after that it will create private filed
   * Map, for creating private field map it will take store value from ES and then a separate map
   * for private field and remove those field from original map. if will user is sending some public
   * field list as well then it will take private field values from another ES index and update
   * values under original data.
   *
   * @param actorMessage
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void profileVisibility(Request actorMessage) {
    Map<String, Object> map = (Map) actorMessage.getRequest();
    String userId = (String) map.get(JsonKey.USER_ID);
    List<String> privateList = (List) map.get(JsonKey.PRIVATE);
    List<String> publicList = (List) map.get(JsonKey.PUBLIC);
    validateFields(privateList, JsonKey.PUBLIC_FIELDS);
    validateFields(publicList, JsonKey.PRIVATE_FIELDS);

    Map<String, Object> esResult = getUserByIdFromES(userId);
    Map<String, Object> esProfileVisibility = getProfileVisibilityByUserIdFromES(userId);
    Map<String, Object> privateDataMap = new HashMap<>();

    esProfileVisibility =
        handlePublicToPrivateConversion(privateList, esResult, esProfileVisibility, privateDataMap);

    handlePrivateToPublicConversion(publicList, esResult, esProfileVisibility);
    updaeProfileVisibility(userId, privateList, publicList, esResult);

    updateDataInES(esResult, esProfileVisibility, userId);
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());
    generateTeleEventForUser(null, userId, "profileVisibility");
  }

  private void updaeProfileVisibility(
      String userId,
      List<String> privateList,
      List<String> publicList,
      Map<String, Object> esResult) {
    Map<String, String> profileVisibilityMap =
        (Map<String, String>) esResult.get(JsonKey.PROFILE_VISIBILITY);
    if (null == profileVisibilityMap) {
      profileVisibilityMap = new HashMap<>();
    }
    prepareProfileVisibilityMap(profileVisibilityMap, privateList, JsonKey.PRIVATE);
    prepareProfileVisibilityMap(profileVisibilityMap, publicList, JsonKey.PUBLIC);

    if (profileVisibilityMap.size() > 0) {
      updateCassandraWithPrivateFiled(userId, profileVisibilityMap);
      esResult.put(JsonKey.PROFILE_VISIBILITY, profileVisibilityMap);
    }
  }

  private void prepareProfileVisibilityMap(
      Map<String, String> profileVisibilityMap, List<String> list, String value) {
    if (list != null) {
      for (String key : list) {
        profileVisibilityMap.put(key, value);
      }
    }
  }

  private void handlePrivateToPublicConversion(
      List<String> publicList,
      Map<String, Object> esResult,
      Map<String, Object> esProfileVisibility) { // now have a check for public field.
    if (publicList != null && !publicList.isEmpty()) {
      // this estype will hold all private data of user.
      // now collecting values from private filed and it will update
      // under original index with public field.
      for (String field : publicList) {
        if (esProfileVisibility.containsKey(field)) {
          esResult.put(field, esProfileVisibility.get(field));
          esProfileVisibility.remove(field);
        } else {
          ProjectLogger.log("field value not found inside private index ==" + field);
        }
      }
    }
  }

  private Map<String, Object> handlePublicToPrivateConversion(
      List<String> privateList,
      Map<String, Object> esResult,
      Map<String, Object> esProfileVisibility,
      Map<String, Object> privateDataMap) {
    if (privateList != null && !privateList.isEmpty()) {
      privateDataMap = handlePrivateVisibility(privateList, esResult, esProfileVisibility);
    }
    if (privateDataMap != null && privateDataMap.size() >= esProfileVisibility.size()) {
      // this will indicate some extra private data is added
      esProfileVisibility = privateDataMap;
      UserUtility.updateProfileVisibilityFields(privateDataMap, esResult);
    }
    return esProfileVisibility;
  }

  private Map<String, Object> getProfileVisibilityByUserIdFromES(String userId) {
    return ElasticSearchUtil.getDataByIdentifier(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.userprofilevisibility.getTypeName(),
        userId);
  }

  private Map<String, Object> getUserByIdFromES(String userId) {
    Map<String, Object> esResult =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId);
    if (esResult == null || esResult.size() == 0) {
      throw new ProjectCommonException(
          ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    return esResult;
  }

  private void validateFields(List<String> values, String listType) {
    // Remove duplicate entries from the list
    // Visibility of permanent fields cannot be changed
    if (CollectionUtils.isNotEmpty(values)) {
      values = values.stream().distinct().collect(Collectors.toList());
      Util.validateProfileVisibilityFields(values, listType, getSystemSettingActorRef());
    }
  }

  /**
   * This method will first removed the remove the saved private data for the user and then it will
   * create new private data for that user.
   *
   * @param dataMap Map<String, Object> allData
   * @param privateDataMap Map<String, Object> only private data.
   * @param userId String
   * @return boolean
   */
  private void updateDataInES(
      Map<String, Object> dataMap, Map<String, Object> privateDataMap, String userId) {
    ElasticSearchUtil.createData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.userprofilevisibility.getTypeName(),
        userId,
        privateDataMap);
    ElasticSearchUtil.createData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(),
        userId,
        dataMap);
  }

  /**
   * THis methods will update user private field under cassandra.
   *
   * @param userId Stirng
   * @param privateFieldMap Map<String,String>
   */
  private void updateCassandraWithPrivateFiled(String userId, Map<String, String> privateFieldMap) {

	User updateUserObj  = new User();
	updateUserObj.setId(userId);
	updateUserObj.setProfileVisibility(privateFieldMap);
    Response response = getUserDao().updateUser(updateUserObj);
    String val = (String) response.get(JsonKey.RESPONSE);
    ProjectLogger.log("Private field updated under cassandra==" + val);
  }

  private Map<String, Object> handlePrivateVisibility(
      List<String> privateFieldList, Map<String, Object> data, Map<String, Object> oldPrivateData) {
    Map<String, Object> privateFiledMap = createPrivateFiledMap(data, privateFieldList);
    privateFiledMap.putAll(oldPrivateData);
    return privateFiledMap;
  }

  /**
   * This method will create a private field map and remove those filed from original map.
   *
   * @param map Map<String, Object> complete save data Map
   * @param fields List<String> list of private fields
   * @return Map<String, Object> map of private field with their original values.
   */
  private Map<String, Object> createPrivateFiledMap(Map<String, Object> map, List<String> fields) {
    Map<String, Object> privateMap = new HashMap<>();
    if (fields != null && !fields.isEmpty()) {
      for (String field : fields) {
        /*
         * now if field contains
         * {address.someField,education.someField,jobprofile.someField} then we need to
         * remove those filed
         */
        if (field.contains(JsonKey.ADDRESS + ".")) {
          privateMap.put(JsonKey.ADDRESS, map.get(JsonKey.ADDRESS));
        } else if (field.contains(JsonKey.EDUCATION + ".")) {
          privateMap.put(JsonKey.EDUCATION, map.get(JsonKey.EDUCATION));
        } else if (field.contains(JsonKey.JOB_PROFILE + ".")) {
          privateMap.put(JsonKey.JOB_PROFILE, map.get(JsonKey.JOB_PROFILE));
        } else if (field.contains(JsonKey.SKILLS + ".")) {
          privateMap.put(JsonKey.SKILLS, map.get(JsonKey.SKILLS));
        } else if (field.contains(JsonKey.BADGE_ASSERTIONS + ".")) {
          privateMap.put(JsonKey.BADGE_ASSERTIONS, map.get(JsonKey.BADGE_ASSERTIONS));
        } else {
          if (!map.containsKey(field)) {
            throw new ProjectCommonException(
                ResponseCode.InvalidColumnError.getErrorCode(),
                ResponseCode.InvalidColumnError.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
          }
          privateMap.put(field, map.get(field));
        }
      }
    }
    return privateMap;
  }
}
