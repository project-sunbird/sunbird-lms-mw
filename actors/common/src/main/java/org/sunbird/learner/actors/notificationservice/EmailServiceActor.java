package org.sunbird.learner.actors.notificationservice;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.sunbird.actor.background.BackgroundOperations;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.models.util.mail.SendMail;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.notificationservice.dao.EmailTemplateDao;
import org.sunbird.learner.actors.notificationservice.dao.impl.EmailTemplateDaoImpl;
import org.sunbird.learner.util.Util;

@ActorConfig(
  tasks = {"emailService"},
  asyncTasks = {"emailService"}
)
public class EmailServiceActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private DecryptionService decryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(
          null);
  private EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
          null);

  @Override
  public void onReceive(Request request) throws Throwable {
    if (request.getOperation().equalsIgnoreCase(BackgroundOperations.emailService.name())) {
      sendMail(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  @SuppressWarnings({"unchecked"})
  private void sendMail(Request actorMessage) {
    Map<String, Object> request =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.EMAIL_REQUEST);
    List<String> emails = (List<String>) request.get(JsonKey.RECIPIENT_EMAILS);
    if (CollectionUtils.isNotEmpty(emails)) {
      checkEmailValidity(emails);
    }

    List<String> userIds = (List<String>) request.get(JsonKey.RECIPIENT_USERIDS);
    if (CollectionUtils.isEmpty(userIds)) {
      userIds = new ArrayList<>();
    }
    // Fetch public user emails from Elastic Search based on recipient search query given in
    // request.
    getUserEmailsFromSearchQuery(request, emails, userIds);

    validateUserIds(userIds, emails);

    Map<String, Object> user = null;
    if (CollectionUtils.isNotEmpty(emails)) {
      user = getUserInfo(emails.get(0));
    }

    String name = "";
    if (emails.size() > 1) {
      name = "All";
    } else if (emails.size() == 1) {
      name = StringUtils.capitalize((String) user.get(JsonKey.FIRST_NAME));
    } else {
      name = "";
    }

    // fetch orgname inorder to set in the Template context
    String orgName = getOrgName(request, (String) user.get(JsonKey.USER_ID));

    request.put(JsonKey.NAME, name);
    if (orgName != null) {
      request.put(JsonKey.ORG_NAME, orgName);
    }
    String resMsg = JsonKey.SUCCESS;
    try {
      SendMail.sendMailWithBody(
          emails.toArray(new String[emails.size()]),
          (String) request.get(JsonKey.SUBJECT),
          ProjectUtil.getContext(request),
          getEmailTemplateFile((String) request.get(JsonKey.EMAIL_TEMPLATE_TYPE)));
    } catch (Exception e) {
      resMsg = JsonKey.FAILURE;
      ProjectLogger.log(
          "EmailServiceActor:sendMail: Exception occurred with message = " + e.getMessage(), e);
    }
    Response res = new Response();
    res.put(JsonKey.RESPONSE, resMsg);
    sender().tell(res, self());
  }

  private void validateUserIds(List<String> userIds, List<String> emails) {
    // Fetch private (masked in Elastic Search) user emails from Cassandra DB
    if (CollectionUtils.isNotEmpty(userIds)) {
      List<Map<String, Object>> userList = getUsersFromDB(userIds);
      // if requested userId list and cassandra user list size not same , means requested userId
      // list
      // contains some invalid userId
      List<String> tempUserIdList = new ArrayList<>();
      if (userIds.size() != userList.size()) {
        userList.forEach(
            user -> {
              tempUserIdList.add((String) user.get(JsonKey.ID));
            });
        userIds.forEach(
            userId -> {
              if (!tempUserIdList.contains(userId)) {
                ProjectCommonException.throwClientErrorException(
                    ResponseCode.invalidValue, "Invalid userId " + userId);
                return;
              }
            });
      } else {
        for (Map<String, Object> userMap : userList) {
          String email = (String) userMap.get(JsonKey.EMAIL);
          if (StringUtils.isNotBlank(email)) {
            String decryptedEmail = decryptionService.decryptData(email);
            emails.add(decryptedEmail);
          }
        }
      }
    }
  }

  private String getEmailTemplateFile(String templateName) {
    EmailTemplateDao emailTemplateDao = EmailTemplateDaoImpl.getInstance();
    return emailTemplateDao.getOrDefault(templateName);
  }

  private List<Map<String, Object>> getUsersFromDB(List<String> userIds) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    List<String> userIdList = new ArrayList<>(userIds);
    List<String> fields = new ArrayList<>();
    fields.add(JsonKey.ID);
    fields.add(JsonKey.FIRST_NAME);
    fields.add(JsonKey.EMAIL);
    Response response =
        cassandraOperation.getRecordsByIdsWithSpecifiedColumns(
            usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), fields, userIdList);
    List<Map<String, Object>> userList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    return userList;
  }

  private void getUserEmailsFromSearchQuery(
      Map<String, Object> request, List<String> emails, List<String> userIds) {
    Map<String, Object> recipientSearchQuery =
        (Map<String, Object>) request.get(JsonKey.RECIPIENT_SEARCH_QUERY);
    if (MapUtils.isNotEmpty(recipientSearchQuery)) {
      List<String> fields = new ArrayList<>();
      fields.add(JsonKey.USER_ID);
      fields.add(JsonKey.ENC_EMAIL);
      recipientSearchQuery.put(JsonKey.FIELDS, fields);
      Map<String, Object> esResult =
          ElasticSearchUtil.complexSearch(
              ElasticSearchUtil.createSearchDTO(recipientSearchQuery),
              EsIndex.sunbird.getIndexName(),
              EsType.user.getTypeName());
      if (MapUtils.isNotEmpty(esResult)
          && CollectionUtils.isNotEmpty((List) esResult.get(JsonKey.CONTENT))) {
        List<Map<String, Object>> usersList =
            (List<Map<String, Object>>) esResult.get(JsonKey.CONTENT);
        usersList.forEach(
            user -> {
              if (StringUtils.isNotBlank((String) user.get(JsonKey.ENC_EMAIL))) {
                String email = decryptionService.decryptData((String) user.get(JsonKey.ENC_EMAIL));
                if (ProjectUtil.isEmailvalid(email)) {
                  emails.add(email);
                } else {
                  ProjectLogger.log(
                      "EmailServiceActor:sendMail: Email decryption failed for userId = "
                          + user.get(JsonKey.USER_ID));
                }
              } else {
                // If email is blank (or private) then fetch email from cassandra
                userIds.add((String) user.get(JsonKey.USER_ID));
              }
            });
      }
    }
  }

  private String getOrgName(Map<String, Object> request, String usrId) {
    String orgName = (String) request.get(JsonKey.ORG_NAME);
    if (StringUtils.isNotBlank(orgName)) {
      return orgName;
    }
    Map<String, Object> esUserResult =
        ElasticSearchUtil.getDataByIdentifier(
            EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), usrId);
    if (null != esUserResult) {
      String rootOrgId = (String) esUserResult.get(JsonKey.ROOT_ORG_ID);
      if (!(StringUtils.isBlank(rootOrgId))) {
        Map<String, Object> esOrgResult =
            ElasticSearchUtil.getDataByIdentifier(
                EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName(), rootOrgId);
        if (null != esOrgResult) {
          orgName =
              (esOrgResult.get(JsonKey.ORG_NAME) != null
                  ? (String) esOrgResult.get(JsonKey.ORGANISATION_NAME)
                  : "");
        }
      }
    }
    return orgName;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getUserInfo(String email) {
    String encryptedMail = "";
    try {
      encryptedMail = encryptionService.encryptData(email);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    SearchDTO searchDTO = new SearchDTO();
    Map<String, Object> additionalProperties = new HashMap<>();
    additionalProperties.put(JsonKey.ENC_EMAIL, encryptedMail);
    searchDTO.addAdditionalProperty(JsonKey.FILTERS, additionalProperties);
    Map<String, Object> esResult =
        ElasticSearchUtil.complexSearch(
            searchDTO, EsIndex.sunbird.getIndexName(), EsType.user.getTypeName());
    if (MapUtils.isNotEmpty(esResult)
        && CollectionUtils.isNotEmpty((List) esResult.get(JsonKey.CONTENT))) {
      return ((List<Map<String, Object>>) esResult.get(JsonKey.CONTENT)).get(0);
    } else {
      return Collections.EMPTY_MAP;
    }
  }

  private void checkEmailValidity(List<String> emails) {
    for (String email : emails) {
      if (!ProjectUtil.isEmailvalid(email)) {
        ProjectCommonException.throwClientErrorException(
            ResponseCode.invalidParameterValue,
            MessageFormat.format(
                ResponseCode.invalidParameterValue.getErrorMessage(), email, JsonKey.EMAIL));
        return;
      }
    }
  }
}
