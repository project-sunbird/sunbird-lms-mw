package org.sunbird.user.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.datasecurity.DataMaskingService;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.common.services.ProfileCompletenessService;
import org.sunbird.common.services.impl.ProfileCompletenessFactory;
import org.sunbird.dto.SearchDTO;
import org.sunbird.extension.user.UserExtension;
import org.sunbird.extension.user.impl.UserProviderRegistryImpl;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.SocialMediaType;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;
import org.sunbird.models.user.User;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;

public class UserUtil {

  private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
          null);
  private static DbInfo userDb = Util.dbInfoMap.get(JsonKey.USER_DB);
  private static ObjectMapper mapper = new ObjectMapper();
  private static SSOManager ssoManager = SSOServiceFactory.getInstance();
  private static PropertiesCache propertiesCache = PropertiesCache.getInstance();
  private static DataMaskingService maskingService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getMaskingServiceInstance(
          null);
  private static DecryptionService decService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(
          null);

  private UserUtil() {}

  @SuppressWarnings("unchecked")
  public static void checkPhoneUniqueness(User user, String opType) {
    // Get Phone configuration if not found , by default phone will be unique across
    // the application
    String phoneSetting = DataCacheHandler.getConfigSettings().get(JsonKey.PHONE_UNIQUE);
    if (StringUtils.isNotBlank(phoneSetting) && Boolean.parseBoolean(phoneSetting)) {
      String phone = user.getPhone();
      if (StringUtils.isNotBlank(phone)) {
        try {
          phone = encryptionService.encryptData(phone);
        } catch (Exception e) {
          ProjectLogger.log("Exception occurred while encrypting phone number ", e);
        }
        Response result =
            cassandraOperation.getRecordsByIndexedProperty(
                userDb.getKeySpace(), userDb.getTableName(), (JsonKey.PHONE), phone);
        List<Map<String, Object>> userMapList =
            (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        if (!userMapList.isEmpty()) {
          if (opType.equalsIgnoreCase(JsonKey.CREATE)) {
            throw new ProjectCommonException(
                ResponseCode.PhoneNumberInUse.getErrorCode(),
                ResponseCode.PhoneNumberInUse.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
          } else {
            Map<String, Object> userMap = userMapList.get(0);
            if (!(((String) userMap.get(JsonKey.ID)).equalsIgnoreCase(user.getId()))) {
              throw new ProjectCommonException(
                  ResponseCode.PhoneNumberInUse.getErrorCode(),
                  ResponseCode.PhoneNumberInUse.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
            }
          }
        }
      }
    }
  }

  public static Map<String, Object> validateExternalIdsAndReturnActiveUser(
      Map<String, Object> userMap) {
    String extId = (String) userMap.get(JsonKey.EXTERNAL_ID);
    String provider = (String) userMap.get(JsonKey.EXTERNAL_ID_PROVIDER);
    String idType = (String) userMap.get(JsonKey.EXTERNAL_ID_TYPE);
    Map<String, Object> user = null;
    if ((StringUtils.isBlank((String) userMap.get(JsonKey.USER_ID))
            && StringUtils.isBlank((String) userMap.get(JsonKey.ID)))
        && StringUtils.isNotEmpty(extId)
        && StringUtils.isNotEmpty(provider)
        && StringUtils.isNotEmpty(idType)) {
      user = getUserFromExternalId(userMap);
      if (MapUtils.isEmpty(user)) {
        throw new ProjectCommonException(
            ResponseCode.externalIdNotFound.getErrorCode(),
            ProjectUtil.formatMessage(
                ResponseCode.externalIdNotFound.getErrorMessage(), extId, idType, provider),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else if (StringUtils.isNotBlank((String) userMap.get(JsonKey.USER_ID))
        || StringUtils.isNotBlank((String) userMap.get(JsonKey.ID))) {
      String userId =
          (StringUtils.isNotBlank((String) userMap.get(JsonKey.USER_ID)))
              ? ((String) userMap.get(JsonKey.USER_ID))
              : ((String) userMap.get(JsonKey.ID));
      user =
          ElasticSearchUtil.getDataByIdentifier(
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.user.getTypeName(),
              userId);
      if (MapUtils.isEmpty(user)) {
        throw new ProjectCommonException(
            ResponseCode.userNotFound.getErrorCode(),
            ResponseCode.userNotFound.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    if (MapUtils.isNotEmpty(user)) {
      if (null != user.get(JsonKey.IS_DELETED) && (boolean) (user.get(JsonKey.IS_DELETED))) {
        throw new ProjectCommonException(
            ResponseCode.inactiveUser.getErrorCode(),
            ResponseCode.inactiveUser.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
      if (StringUtils.isNotBlank((String) userMap.get(JsonKey.USER_ID))) {
        userMap.put(JsonKey.ID, user.get(JsonKey.USER_ID));
      } else {
        userMap.put(JsonKey.USER_ID, user.get(JsonKey.ID));
      }
    }
    return user;
  }

  public static void updateUserToRegistry(Map<String, Object> userMap, String registryId) {
    UserExtension userExtension = new UserProviderRegistryImpl();
    if (StringUtils.isNotBlank(registryId)) {
      userMap.put(JsonKey.REGISTRY_ID, registryId);
      userExtension.update(userMap);
    } else {
      userExtension.create(userMap);
    }
  }

  public static Map<String, Object> getUserFromExternalId(Map<String, Object> userMap) {
    Map<String, Object> user = null;
    Map<String, Object> externalIdReq = new WeakHashMap<>();
    externalIdReq.put(
        JsonKey.PROVIDER, ((String) userMap.get(JsonKey.EXTERNAL_ID_PROVIDER)).toLowerCase());
    externalIdReq.put(
        JsonKey.ID_TYPE, ((String) userMap.get(JsonKey.EXTERNAL_ID_TYPE)).toLowerCase());
    externalIdReq.put(
        JsonKey.EXTERNAL_ID,
        encryptData(((String) userMap.get(JsonKey.EXTERNAL_ID)).toLowerCase()));
    Response response =
        cassandraOperation.getRecordsByCompositeKey(
            JsonKey.SUNBIRD, JsonKey.USR_EXT_IDNT_TABLE, externalIdReq);
    List<Map<String, Object>> userRecordList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);

    if (CollectionUtils.isNotEmpty(userRecordList)) {
      Map<String, Object> userExtIdRecord = userRecordList.get(0);
      user =
          ElasticSearchUtil.getDataByIdentifier(
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.user.getTypeName(),
              (String) userExtIdRecord.get(JsonKey.USER_ID));
    }
    return user;
  }

  @SuppressWarnings("unchecked")
  public static void checkEmailUniqueness(User user, String opType) {
    // Get Email configuration if not found , by default Email can be duplicate
    // across the
    // application
    String emailSetting = DataCacheHandler.getConfigSettings().get(JsonKey.EMAIL_UNIQUE);
    if (StringUtils.isNotBlank(emailSetting) && Boolean.parseBoolean(emailSetting)) {
      String email = user.getEmail();
      if (StringUtils.isNotBlank(email)) {
        try {
          email = encryptionService.encryptData(email);
        } catch (Exception e) {
          ProjectLogger.log("Exception occurred while encrypting Email ", e);
        }
        Map<String, Object> filters = new HashMap<>();
        filters.put(JsonKey.ENC_EMAIL, email);
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.FILTERS, filters);
        SearchDTO searchDto = Util.createSearchDto(map);
        Map<String, Object> result =
            ElasticSearchUtil.complexSearch(
                searchDto,
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.user.getTypeName());
        List<Map<String, Object>> userMapList =
            (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
        if (!userMapList.isEmpty()) {
          if (opType.equalsIgnoreCase(JsonKey.CREATE)) {
            throw new ProjectCommonException(
                ResponseCode.emailInUse.getErrorCode(),
                ResponseCode.emailInUse.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
          } else {
            Map<String, Object> userMap = userMapList.get(0);
            if (!(((String) userMap.get(JsonKey.ID)).equalsIgnoreCase(user.getId()))) {
              throw new ProjectCommonException(
                  ResponseCode.emailInUse.getErrorCode(),
                  ResponseCode.emailInUse.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
            }
          }
        }
      }
    }
  }

  public static void validateUserPhoneEmailAndWebPages(User user, String operationType) {
    checkPhoneUniqueness(user, operationType);
    checkEmailUniqueness(user, operationType);
    if (CollectionUtils.isNotEmpty(user.getWebPages())) {
      SocialMediaType.validateSocialMedia(user.getWebPages());
    }
  }

  public static void checkUserExistOrNot(User user) {
    Map<String, Object> searchQueryMap = new HashMap<>();
    // loginId is encrypted in our application
    searchQueryMap.put(JsonKey.LOGIN_ID, getEncryptedData(user.getLoginId()));
    if (CollectionUtils.isNotEmpty(searchUser(searchQueryMap))) {
      throw new ProjectCommonException(
          ResponseCode.userAlreadyExists.getErrorCode(),
          ProjectUtil.formatMessage(
              ResponseCode.userAlreadyExists.getErrorMessage(), JsonKey.USERNAME),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  public static String getEncryptedData(String value) {
    try {
      return encryptionService.encryptData(value);
    } catch (Exception e) {
      throw new ProjectCommonException(
          ResponseCode.userDataEncryptionError.getErrorCode(),
          ResponseCode.userDataEncryptionError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  @SuppressWarnings("unchecked")
  private static List<User> searchUser(Map<String, Object> searchQueryMap) {
    List<User> userList = new ArrayList<>();
    Map<String, Object> searchRequestMap = new HashMap<>();
    searchRequestMap.put(JsonKey.FILTERS, searchQueryMap);
    SearchDTO searchDto = Util.createSearchDto(searchRequestMap);
    String[] types = {ProjectUtil.EsType.user.getTypeName()};
    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            searchDto, ProjectUtil.EsIndex.sunbird.getIndexName(), types);
    if (MapUtils.isNotEmpty(result)) {
      List<Map<String, Object>> searchResult =
          (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
      if (CollectionUtils.isNotEmpty(searchResult)) {
        userList =
            searchResult
                .stream()
                .map(s -> mapper.convertValue(s, User.class))
                .collect(Collectors.toList());
      }
    }
    return userList;
  }

  public static List<Map<String, String>> copyAndConvertExternalIdsToLower(
      List<Map<String, String>> externalIds) {
    List<Map<String, String>> list = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(externalIds)) {
      storeOriginalExternalIdsValue(externalIds);
      list = convertExternalIdsValueToLowerCase(externalIds);
    }
    return list;
  }

  public static void storeOriginalExternalIdsValue(List<Map<String, String>> externalIds) {
    externalIds.forEach(
        externalIdMap -> {
          externalIdMap.put(JsonKey.ORIGINAL_EXTERNAL_ID, externalIdMap.get(JsonKey.ID));
          externalIdMap.put(JsonKey.ORIGINAL_PROVIDER, externalIdMap.get(JsonKey.PROVIDER));
          externalIdMap.put(JsonKey.ORIGINAL_ID_TYPE, externalIdMap.get(JsonKey.ID_TYPE));
        });
  }

  public static List<Map<String, String>> convertExternalIdsValueToLowerCase(
      List<Map<String, String>> externalIds) {
    ConvertValuesToLowerCase mapper =
        s -> {
          s.put(JsonKey.ID, s.get(JsonKey.ID).toLowerCase());
          s.put(JsonKey.PROVIDER, s.get(JsonKey.PROVIDER).toLowerCase());
          s.put(JsonKey.ID_TYPE, s.get(JsonKey.ID_TYPE).toLowerCase());
          return s;
        };
    return externalIds.stream().map(s -> mapper.convertToLowerCase(s)).collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  public static void checkExternalIdUniqueness(User user, String operation) {
    if (CollectionUtils.isNotEmpty(user.getExternalIds())) {
      for (Map<String, String> externalId : user.getExternalIds()) {
        if (StringUtils.isNotBlank(externalId.get(JsonKey.ID))
            && StringUtils.isNotBlank(externalId.get(JsonKey.PROVIDER))
            && StringUtils.isNotBlank(externalId.get(JsonKey.ID_TYPE))) {
          Map<String, Object> externalIdReq = new HashMap<>();
          externalIdReq.put(JsonKey.PROVIDER, externalId.get(JsonKey.PROVIDER));
          externalIdReq.put(JsonKey.ID_TYPE, externalId.get(JsonKey.ID_TYPE));
          externalIdReq.put(JsonKey.EXTERNAL_ID, encryptData(externalId.get(JsonKey.ID)));
          Response response =
              cassandraOperation.getRecordsByCompositeKey(
                  JsonKey.SUNBIRD, JsonKey.USR_EXT_IDNT_TABLE, externalIdReq);
          List<Map<String, Object>> externalIdsRecord =
              (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
          if (CollectionUtils.isNotEmpty(externalIdsRecord)) {
            if (JsonKey.CREATE.equalsIgnoreCase(operation)) {
              throwUserAlreadyExistsException(
                  externalId.get(JsonKey.ID),
                  externalId.get(JsonKey.ID_TYPE),
                  externalId.get(JsonKey.PROVIDER));
            } else if (JsonKey.UPDATE.equalsIgnoreCase(operation)) {
              // If end user will try to add,edit or remove other user extIds throw exception
              String userId = (String) externalIdsRecord.get(0).get(JsonKey.USER_ID);
              if (!(user.getUserId().equalsIgnoreCase(userId))) {
                if (JsonKey.ADD.equalsIgnoreCase(externalId.get(JsonKey.OPERATION))
                    || StringUtils.isBlank(externalId.get(JsonKey.OPERATION))) {
                  throw new ProjectCommonException(
                      ResponseCode.externalIdAssignedToOtherUser.getErrorCode(),
                      ProjectUtil.formatMessage(
                          ResponseCode.externalIdAssignedToOtherUser.getErrorMessage(),
                          externalId.get(JsonKey.ID),
                          externalId.get(JsonKey.ID_TYPE),
                          externalId.get(JsonKey.PROVIDER)),
                      ResponseCode.CLIENT_ERROR.getResponseCode());
                } else {
                  throwExternalIDNotFoundException(
                      externalId.get(JsonKey.ID),
                      externalId.get(JsonKey.ID_TYPE),
                      externalId.get(JsonKey.PROVIDER));
                }
              }
            }
          } else {
            // if user will try to delete non existing extIds
            if (JsonKey.UPDATE.equalsIgnoreCase(operation)
                && JsonKey.REMOVE.equalsIgnoreCase(externalId.get(JsonKey.OPERATION))) {
              throwExternalIDNotFoundException(
                  externalId.get(JsonKey.ID),
                  externalId.get(JsonKey.ID_TYPE),
                  externalId.get(JsonKey.PROVIDER));
            }
          }
        }
      }
    }
  }

  private static void throwUserAlreadyExistsException(
      String externalId, String idType, String provider) {
    throw new ProjectCommonException(
        ResponseCode.userAlreadyExists.getErrorCode(),
        ProjectUtil.formatMessage(
            ResponseCode.userAlreadyExists.getErrorMessage(),
            ProjectUtil.formatMessage(
                ResponseMessage.Message.EXTERNAL_ID_FORMAT, externalId, idType, provider)),
        ResponseCode.CLIENT_ERROR.getResponseCode());
  }

  private static void throwExternalIDNotFoundException(
      String externalId, String idType, String provider) {
    throw new ProjectCommonException(
        ResponseCode.externalIdNotFound.getErrorCode(),
        ProjectUtil.formatMessage(
            ResponseCode.externalIdNotFound.getErrorMessage(), externalId, idType, provider),
        ResponseCode.CLIENT_ERROR.getResponseCode());
  }

  public static String encryptData(String value) {
    try {
      return encryptionService.encryptData(value);
    } catch (Exception e) {
      throw new ProjectCommonException(
          ResponseCode.userDataEncryptionError.getErrorCode(),
          ResponseCode.userDataEncryptionError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  public static void upsertUserInKeycloak(Map<String, Object> userMap, String operationType) {
    if (JsonKey.CREATE.equalsIgnoreCase(operationType)) {
      String userId = "";
      Map<String, String> responseMap = ssoManager.createUser(userMap);
      userId = responseMap.get(JsonKey.USER_ID);
      if (!StringUtils.isBlank(userId)) {
        userMap.put(JsonKey.USER_ID, userId);
        userMap.put(JsonKey.ID, userId);
      } else {
        throw new ProjectCommonException(
            ResponseCode.userRegUnSuccessfull.getErrorCode(),
            ResponseCode.userRegUnSuccessfull.getErrorMessage(),
            ResponseCode.SERVER_ERROR.getResponseCode());
      }
    } else {
      String response = ssoManager.updateUser(userMap);
      if (!(!StringUtils.isBlank(response) && response.equalsIgnoreCase(JsonKey.SUCCESS))) {
        throw new ProjectCommonException(
            ResponseCode.userUpdationUnSuccessfull.getErrorCode(),
            ResponseCode.userUpdationUnSuccessfull.getErrorMessage(),
            ResponseCode.SERVER_ERROR.getResponseCode());
      }
    }
  }

  public static void addMaskEmailAndPhone(Map<String, Object> userMap) {
    String phone = (String) userMap.get(JsonKey.PHONE);
    String email = (String) userMap.get(JsonKey.EMAIL);
    if (!StringUtils.isBlank(phone)) {
      userMap.put(JsonKey.ENC_PHONE, phone);
      userMap.put(JsonKey.PHONE, maskingService.maskPhone(decService.decryptData(phone)));
    }
    if (!StringUtils.isBlank(email)) {
      userMap.put(JsonKey.ENC_EMAIL, email);
      userMap.put(JsonKey.EMAIL, maskingService.maskEmail(decService.decryptData(email)));
    }
  }

  public static void updateUserExtId(Map<String, Object> requestMap) {
    List<Map<String, String>> dbResExternalIds =
        getUserExternalIds((String) requestMap.get(JsonKey.USER_ID));
    List<Map<String, String>> externalIds =
        (List<Map<String, String>>) requestMap.get(JsonKey.EXTERNAL_IDS);
    if (CollectionUtils.isNotEmpty(externalIds)) {
      // will not allow user to update idType value, if user will try to update idType will
      // ignore
      // user will have only one entry for a idType for given provider so get extId based on idType
      // List of idType values for a user will distinct and unique
      for (Map<String, String> extIdMap : externalIds) {
        Optional<Map<String, String>> extMap = checkExternalID(dbResExternalIds, extIdMap);
        Map<String, String> map = extMap.orElse(null);
        // Allowed operation type for externalIds ("add", "remove", "edit")
        if (JsonKey.ADD.equalsIgnoreCase(extIdMap.get(JsonKey.OPERATION))
            || StringUtils.isBlank(extIdMap.get(JsonKey.OPERATION))) {
          if (MapUtils.isEmpty(map)) {
            upsertUserExternalIdentityData(extIdMap, requestMap, JsonKey.CREATE);
          } else {
            // if external Id with same provider and idType exist then delete first then update
            // to update user externalId first we need to delete the record as externalId is the
            // part of composite key
            deleteUserExternalId(requestMap, map);
            upsertUserExternalIdentityData(extIdMap, requestMap, JsonKey.UPDATE);
          }
        } else {
          // operation is either edit or remove
          if (MapUtils.isNotEmpty(map)) {
            if (JsonKey.REMOVE.equalsIgnoreCase(extIdMap.get(JsonKey.OPERATION))) {
              if (StringUtils.isNotBlank(map.get(JsonKey.ID_TYPE))
                  && StringUtils.isNotBlank((String) requestMap.get(JsonKey.USER_ID))
                  && StringUtils.isNotBlank(map.get(JsonKey.PROVIDER))) {
                deleteUserExternalId(requestMap, map);
              }
            } else if (JsonKey.EDIT.equalsIgnoreCase(extIdMap.get(JsonKey.OPERATION))) {
              // to update user externalId first we need to delete the record as externalId is the
              // part of composite key
              deleteUserExternalId(requestMap, map);
              upsertUserExternalIdentityData(extIdMap, requestMap, JsonKey.UPDATE);
            }
          } else {
            throwExternalIDNotFoundException(
                extIdMap.get(JsonKey.ID),
                extIdMap.get(JsonKey.ID_TYPE),
                extIdMap.get(JsonKey.PROVIDER));
          }
        }
      }
    }
  }

  private static void upsertUserExternalIdentityData(
      Map<String, String> extIdsMap, Map<String, Object> requestMap, String operation) {
    try {
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.EXTERNAL_ID, encryptData(extIdsMap.get(JsonKey.ID)));
      map.put(
          JsonKey.ORIGINAL_EXTERNAL_ID, encryptData(extIdsMap.get(JsonKey.ORIGINAL_EXTERNAL_ID)));
      map.put(JsonKey.PROVIDER, extIdsMap.get(JsonKey.PROVIDER));
      map.put(JsonKey.ORIGINAL_PROVIDER, extIdsMap.get(JsonKey.ORIGINAL_PROVIDER));
      map.put(JsonKey.ID_TYPE, extIdsMap.get(JsonKey.ID_TYPE));
      map.put(JsonKey.ORIGINAL_ID_TYPE, extIdsMap.get(JsonKey.ORIGINAL_ID_TYPE));
      map.put(JsonKey.USER_ID, requestMap.get(JsonKey.USER_ID));
      if (JsonKey.CREATE.equalsIgnoreCase(operation)) {
        map.put(JsonKey.CREATED_BY, requestMap.get(JsonKey.CREATED_BY));
        map.put(JsonKey.CREATED_ON, new Timestamp(Calendar.getInstance().getTime().getTime()));
      } else {
        map.put(JsonKey.LAST_UPDATED_BY, requestMap.get(JsonKey.UPDATED_BY));
        map.put(JsonKey.LAST_UPDATED_ON, new Timestamp(Calendar.getInstance().getTime().getTime()));
      }
      cassandraOperation.upsertRecord(JsonKey.SUNBIRD, JsonKey.USR_EXT_IDNT_TABLE, map);
    } catch (Exception ex) {
      ProjectLogger.log("Util:upsertUserExternalIdentityData : Exception occurred", ex);
    }
  }

  private static void deleteUserExternalId(
      Map<String, Object> requestMap, Map<String, String> map) {
    map.remove(JsonKey.LAST_UPDATED_BY);
    map.remove(JsonKey.CREATED_BY);
    map.remove(JsonKey.LAST_UPDATED_ON);
    map.remove(JsonKey.CREATED_ON);
    map.remove(JsonKey.USER_ID);
    map.remove(JsonKey.ORIGINAL_EXTERNAL_ID);
    map.remove(JsonKey.ORIGINAL_ID_TYPE);
    map.remove(JsonKey.ORIGINAL_PROVIDER);
    cassandraOperation.deleteRecord(JsonKey.SUNBIRD, JsonKey.USR_EXT_IDNT_TABLE, map);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> encryptUserData(Map<String, Object> userMap) {
    try {
      UserUtility.encryptUserData(userMap);
    } catch (Exception e1) {
      throw new ProjectCommonException(
          ResponseCode.userDataEncryptionError.getErrorCode(),
          ResponseCode.userDataEncryptionError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
    Map<String, Object> requestMap = new HashMap<>();
    User user = mapper.convertValue(userMap, User.class);
    requestMap.putAll(mapper.convertValue(user, Map.class));
    return requestMap;
  }

  public static Map<String, Object> checkProfileCompleteness(Map<String, Object> userMap) {
    ProfileCompletenessService profileService = ProfileCompletenessFactory.getInstance();
    return profileService.computeProfile(userMap);
  }

  public static void setUserDefaultValue(Map<String, Object> userMap) {
    /** will ignore roles coming from req, Only public role is applicable for user by default */
    List<String> roles = new ArrayList<>();
    roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
    userMap.put(JsonKey.ROLES, roles);
    // update db with emailVerified as false (default)
    userMap.put(JsonKey.EMAIL_VERIFIED, false);
    if (!StringUtils.isBlank((String) userMap.get(JsonKey.COUNTRY_CODE))) {
      userMap.put(
          JsonKey.COUNTRY_CODE, propertiesCache.getProperty(JsonKey.SUNBIRD_DEFAULT_COUNTRY_CODE));
    }
    // Since global settings are introduced, profile visibility map should be empty during user
    // creation
    userMap.put(JsonKey.PROFILE_VISIBILITY, new HashMap<String, String>());
    userMap.put(JsonKey.IS_DELETED, false);
    // create loginId to ensure uniqueness for combination of userName and channel
    String loginId = Util.getLoginId(userMap);
    userMap.put(JsonKey.LOGIN_ID, loginId);
    userMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    userMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
  }

  public static void validateExternalIds(User user, String operationType) {
    if (CollectionUtils.isNotEmpty(user.getExternalIds())) {
      List<Map<String, String>> list = copyAndConvertExternalIdsToLower(user.getExternalIds());
      user.setExternalIds(list);
    }
    checkExternalIdUniqueness(user, operationType);
    if (JsonKey.UPDATE.equalsIgnoreCase(operationType)
        && CollectionUtils.isNotEmpty(user.getExternalIds())) {
      validateUserExternalIds(user);
    }
  }

  @SuppressWarnings("unchecked")
  public static void checkEmailSameOrDiff(Map<String, Object> userMap) {
    if (StringUtils.isNotBlank((String) userMap.get(JsonKey.EMAIL))) {
      Response response =
          cassandraOperation.getRecordById(
              userDb.getKeySpace(), userDb.getTableName(), (String) userMap.get(JsonKey.ID));
      List<Map<String, Object>> resList =
          (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if (!resList.isEmpty()) {
        Map<String, Object> res = resList.get(0);
        String email = (String) res.get(JsonKey.EMAIL);
        String encEmail = (String) userMap.get(JsonKey.EMAIL);
        try {
          encEmail = encryptionService.encryptData((String) userMap.get(JsonKey.EMAIL));
        } catch (Exception ex) {
          ProjectLogger.log("Exception occurred while encrypting user email.");
        }
        if ((encEmail).equalsIgnoreCase(email)) {
          userMap.remove(JsonKey.EMAIL);
        }
      }
    }
  }

  private static Optional<Map<String, String>> checkExternalID(
      List<Map<String, String>> dbResExternalIds, Map<String, String> extIdMap) {
    Optional<Map<String, String>> extMap =
        dbResExternalIds
            .stream()
            .filter(
                s -> {
                  if (((s.get(JsonKey.ID_TYPE)).equalsIgnoreCase(extIdMap.get(JsonKey.ID_TYPE)))
                      && ((s.get(JsonKey.PROVIDER))
                          .equalsIgnoreCase(extIdMap.get(JsonKey.PROVIDER)))) {
                    return true;
                  } else {
                    return false;
                  }
                })
            .findFirst();
    return extMap;
  }

  public static void validateUserExternalIds(User user) {
    List<Map<String, String>> dbResExternalIds = getUserExternalIds(user.getUserId());
    List<Map<String, String>> externalIds = user.getExternalIds();
    if (CollectionUtils.isNotEmpty(externalIds)) {
      for (Map<String, String> extIdMap : externalIds) {
        Optional<Map<String, String>> extMap = checkExternalID(dbResExternalIds, extIdMap);
        Map<String, String> map = extMap.orElse(null);
        // Allowed operation type for externalIds ("add", "remove", "edit")
        if (!(JsonKey.ADD.equalsIgnoreCase(extIdMap.get(JsonKey.OPERATION))
            || StringUtils.isBlank(extIdMap.get(JsonKey.OPERATION)))) {
          // operation is either edit or remove
          if (MapUtils.isEmpty(map)) {
            throwExternalIDNotFoundException(
                extIdMap.get(JsonKey.ID),
                extIdMap.get(JsonKey.ID_TYPE),
                extIdMap.get(JsonKey.PROVIDER));
          }
        }
      }
    }
  }

  private static List<Map<String, String>> getUserExternalIds(String userId) {
    List<Map<String, String>> dbResExternalIds = new ArrayList<>();
    Response response =
        cassandraOperation.getRecordsByIndexedProperty(
            JsonKey.SUNBIRD, JsonKey.USR_EXT_IDNT_TABLE, JsonKey.USER_ID, userId);
    if (null != response && null != response.getResult()) {
      dbResExternalIds = (List<Map<String, String>>) response.getResult().get(JsonKey.RESPONSE);
    }
    return dbResExternalIds;
  }

  public static List<Map<String, Object>> getAddressDetails(String userId, String addressId) {
    Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    List<Map<String, Object>> userAddressList = new ArrayList<>();
    Response addrResponse = null;
    try {
      if (StringUtils.isNotBlank(userId)) {
        ProjectLogger.log("collecting user address operation user Id : " + userId);
        String encUserId = encryptData(userId);
        addrResponse =
            cassandraOperation.getRecordsByIndexedProperty(
                addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), JsonKey.USER_ID, encUserId);
      } else {
        addrResponse =
            cassandraOperation.getRecordById(
                addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addressId);
      }
      userAddressList = (List<Map<String, Object>>) addrResponse.getResult().get(JsonKey.RESPONSE);
      ProjectLogger.log("collecting user address operation completed user Id : " + userId);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return userAddressList;
  }

  @SuppressWarnings("unchecked")
  public static List<Map<String, Object>> getUserOrgDetails(String userId) {
    List<Map<String, Object>> userOrgList = null;
    List<Map<String, Object>> organisations = new ArrayList<>();
    try {
      Map<String, Object> reqMap = new WeakHashMap<>();
      reqMap.put(JsonKey.USER_ID, userId);
      reqMap.put(JsonKey.IS_DELETED, false);
      Util.DbInfo orgUsrDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
      Response result =
          cassandraOperation.getRecordsByProperties(
              orgUsrDbInfo.getKeySpace(), orgUsrDbInfo.getTableName(), reqMap);
      userOrgList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (CollectionUtils.isNotEmpty(userOrgList)) {
        for (Map<String, Object> tempMap : userOrgList) {
          organisations.add(tempMap);
        }
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return organisations;
  }
}

@FunctionalInterface
interface ConvertValuesToLowerCase {
  Map<String, String> convertToLowerCase(Map<String, String> map);
}
