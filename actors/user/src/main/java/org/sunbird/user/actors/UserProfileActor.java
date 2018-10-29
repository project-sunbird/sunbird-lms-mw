package org.sunbird.user.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.SocialMediaType;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;

import com.typesafe.config.Config;

import akka.actor.ActorRef;

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
        break;
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

    Map<String, Object> esPublicUserProfile = getUserService().esGetUserById(userId);
    Map<String, Object> esPrivateUserProfile =
        getUserService().esGetProfileVisibilityByUserId(userId);

    updateUserProfile(publicList, privateList, esPublicUserProfile, esPrivateUserProfile);
    updateProfileVisibility(userId, publicList, privateList, esPublicUserProfile);

    getUserService().syncProfileVisibility(userId, esPublicUserProfile, esPrivateUserProfile);
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());
    generateTelemetryEvent(null, userId, "profileVisibility");
  }

  private void updateProfileVisibility(
      String userId,
      List<String> publicList,
      List<String> privateList,
      Map<String, Object> esPublicUserProfile) {
    Map<String, String> profileVisibilityMap =
        (Map<String, String>) esPublicUserProfile.get(JsonKey.PROFILE_VISIBILITY);
    if (null == profileVisibilityMap) {
      profileVisibilityMap = new HashMap<>();
    }

    prepareProfileVisibilityMap(profileVisibilityMap, publicList, JsonKey.PUBLIC);
    prepareProfileVisibilityMap(profileVisibilityMap, privateList, JsonKey.PRIVATE);

    if (profileVisibilityMap.size() > 0) {
      updateCassandraWithPrivateFiled(userId, profileVisibilityMap);
      esPublicUserProfile.put(JsonKey.PROFILE_VISIBILITY, profileVisibilityMap);
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

  private void addRemovedPrivateFieldsInPublicUserProfile(
      List<String> publicList,
      Map<String, Object> esPublicUserProfile,
      Map<String, Object> esPrivateUserProfile) { // now have a check for public field.
    if (CollectionUtils.isNotEmpty(publicList)) {
      // this estype will hold all private data of user.
      // now collecting values from private filed and it will update
      // under original index with public field.
      for (String field : publicList) {
        if (esPrivateUserProfile.containsKey(field)) {
          esPublicUserProfile.put(field, esPrivateUserProfile.get(field));
          esPrivateUserProfile.remove(field);
        } else {
          ProjectLogger.log("field value not found inside private index ==" + field);
        }
      }
    }
  }

  private void updateUserProfile(
      List<String> publicList,
      List<String> privateList,
      Map<String, Object> esPublicUserProfile,
      Map<String, Object> esPrivateUserProfile) {
    Map<String, Object> privateDataMap = null;
    if (CollectionUtils.isNotEmpty(privateList)) {
      privateDataMap = getPrivateFieldMap(privateList, esPublicUserProfile, esPrivateUserProfile);
    }
    if (privateDataMap != null && privateDataMap.size() > 0) {
      resetPrivateFieldsInPublicUserProfile(privateDataMap, esPublicUserProfile);
    }
    addRemovedPrivateFieldsInPublicUserProfile(
        publicList, esPublicUserProfile, esPrivateUserProfile);
  }

  private void validateFields(List<String> values, String listType) {
    // Remove duplicate entries from the list
    // Visibility of permanent fields cannot be changed
    if (CollectionUtils.isNotEmpty(values)) {
      List<String> distValues = values.stream().distinct().collect(Collectors.toList());
      validateProfileVisibilityFields(distValues, listType, getSystemSettingActorRef());
    }
  }

  /**
   * THis methods will update user private field under cassandra.
   *
   * @param userId Stirng
   * @param privateFieldMap Map<String,String>
   */
  private void updateCassandraWithPrivateFiled(String userId, Map<String, String> privateFieldMap) {

    User updateUserObj = new User();
    updateUserObj.setId(userId);
    updateUserObj.setProfileVisibility(privateFieldMap);
    Response response = getUserDao().updateUser(updateUserObj);
    String val = (String) response.get(JsonKey.RESPONSE);
    ProjectLogger.log("Private field updated under cassandra==" + val);
  }

  private Map<String, Object> getPrivateFieldMap(
      List<String> privateFieldList, Map<String, Object> data, Map<String, Object> oldPrivateData) {
    Map<String, Object> privateFiledMap = createPrivateFieldMap(data, privateFieldList);
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
  private Map<String, Object> createPrivateFieldMap(Map<String, Object> map, List<String> fields) {
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

  private Map<String, Object> resetPrivateFieldsInPublicUserProfile(
      Map<String, Object> privateDataMap, Map<String, Object> esPublicUserProfile) {
    for (String field : privateDataMap.keySet()) {
      if ("dob".equalsIgnoreCase(field)) {
        esPublicUserProfile.put(field, null);
      } else if (privateDataMap.get(field) instanceof List) {
        esPublicUserProfile.put(field, new ArrayList<>());
      } else if (privateDataMap.get(field) instanceof Map) {
        esPublicUserProfile.put(field, new HashMap<>());
      } else if (privateDataMap.get(field) instanceof String) {
        esPublicUserProfile.put(field, "");
      } else {
        esPublicUserProfile.put(field, null);
      }
    }
    return esPublicUserProfile;
  }

  public void validateProfileVisibilityFields(
      List<String> fieldList, String fieldTypeKey, ActorRef actorRef) {
    String conflictingFieldTypeKey =
        JsonKey.PUBLIC_FIELDS.equalsIgnoreCase(fieldTypeKey) ? JsonKey.PRIVATE : JsonKey.PUBLIC;

    Config userProfileConfig = Util.getUserProfileConfig(actorRef);

    List<String> fields = userProfileConfig.getStringList(fieldTypeKey);
    List<String> fieldsCopy = new ArrayList<String>(fields);
    fieldsCopy.retainAll(fieldList);

    if (!fieldsCopy.isEmpty()) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.invalidParameterValue,
          ProjectUtil.formatMessage(
              ResponseCode.invalidParameterValue.getErrorMessage(),
              fieldsCopy.toString(),
              StringFormatter.joinByDot(JsonKey.PROFILE_VISIBILITY, conflictingFieldTypeKey)));
    }
  }
}
