package org.sunbird.learner.actors.notificationservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.stream.Collectors;
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

    List<String> tempUserIdList = new ArrayList<>();
    List<String> userIds = (List<String>) request.get(JsonKey.RECIPIENT_USERIDS);
    if (CollectionUtils.isEmpty(userIds)) {
      userIds = new ArrayList<>();
    }
    // fetch user email from cassandra and if email is marked as private then get user id and fetch
    // from cassandra
    // get user from recepient search query
    getUserEmails(request, emails, userIds);
    String name = "";
    if (CollectionUtils.isNotEmpty(userIds)) {
      // get user details from cassandra
      List<Map<String, Object>> userList = getUserList(userIds);
      // if requested userId list and cassandra user list size not same , means requested userId
      // list
      // contains some invalid userId
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
        for (Map<String, Object> map : userList) {
          String email = (String) map.get(JsonKey.EMAIL);
          if (StringUtils.isNotBlank(email)) {
            String decryptedEmail = decryptionService.decryptData(email);
            emails.add(decryptedEmail);
          }
        }
      }
      // This name will be used only in case of sending email to single person.
      name = (String) userList.get(0).get(JsonKey.FIRST_NAME);
    }

    if (emails.size() > 1) {
      name = "All";
    } else if (StringUtils.isBlank(name)) {
      name = "";
    } else {
      name = StringUtils.capitalize(name);
    }

    // fetch orgname inorder to set in the Template context
    String orgName = (String) request.get(JsonKey.ORG_NAME);
    if (null == orgName && !tempUserIdList.isEmpty()) {
      String usrId = tempUserIdList.get(0);
      orgName = getOrgName(usrId);
    }
    request.put(JsonKey.NAME, name);
    if (orgName != null) {
      request.put(JsonKey.ORG_NAME, orgName);
    }
    String resMsg = JsonKey.SUCCESS;
    try {
      SendMail.sendMail(
          emails.toArray(new String[emails.size()]),
          (String) request.get(JsonKey.SUBJECT),
          ProjectUtil.getContext(request),
          (String) request.get(JsonKey.EMAIL_TEMPLATE_TYPE),
          getEmailTemplateFile((String) request.get(JsonKey.EMAIL_TEMPLATE_TYPE)));
    } catch (Exception e) {
      resMsg = JsonKey.FAILURE;
      ProjectLogger.log("EmailServiceActor:sendMail: " + e.getMessage(), e);
    }
    Response res = new Response();
    res.put(JsonKey.RESPONSE, resMsg);
    sender().tell(res, self());
  }

  private String getEmailTemplateFile(String templateName) {
    Map<String, Object> queryMap = new WeakHashMap<>();
    queryMap.put(JsonKey.NAME, templateName);
    Response response = cassandraOperation.getRecordById("sunbird", "email_template", queryMap);
    List<Map<String, Object>> respMapList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    Map<String, Object> map = respMapList.get(0);
    String fileContent = (String) map.get("template");
    return fileContent;
  }

  private List<Map<String, Object>> getUserList(List<String> userIds) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> propertyMap = new WeakHashMap<String, Object>();
    propertyMap.put(JsonKey.ID, new ArrayList<>(userIds));
    List<String> fields = new ArrayList<>();
    fields.add(JsonKey.ID);
    fields.add(JsonKey.FIRST_NAME);
    fields.add(JsonKey.EMAIL);
    Response response =
        cassandraOperation.getRecordsByProperties(
            usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), propertyMap, fields);
    List<Map<String, Object>> respMapList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    return respMapList;
  }

  private void getUserEmails(
      Map<String, Object> request, List<String> emails, List<String> userIds) {
    // get userId and email from search query
    Map<String, Object> recipientSearchQuery =
        (Map<String, Object>) request.get(JsonKey.RECIPIENT_SEARCH_QUERY);
    if (MapUtils.isNotEmpty(recipientSearchQuery)) {
      List<String> fields = new ArrayList<>();
      fields.add(JsonKey.USER_ID);
      fields.add(JsonKey.ENC_EMAIL);
      fields.add(JsonKey.FIRST_NAME);
      recipientSearchQuery.put(JsonKey.FIELDS, fields);
      Map<String, Object> esResult =
          ElasticSearchUtil.complexSearch(
              ElasticSearchUtil.createSearchDTO(recipientSearchQuery),
              EsIndex.sunbird.getIndexName(),
              EsType.user.getTypeName());
      if (MapUtils.isNotEmpty(esResult)
          && CollectionUtils.isNotEmpty((List) esResult.get(JsonKey.CONTENT))) {
        List<Map<String, Object>> esUserEmailList =
            (List<Map<String, Object>>) esResult.get(JsonKey.CONTENT);
        esUserEmailList.forEach(
            user -> {
              if (StringUtils.isNotBlank((String) user.get(JsonKey.ENC_EMAIL))) {
                String email = decryptionService.decryptData((String) user.get(JsonKey.ENC_EMAIL));
                if (ProjectUtil.isEmailvalid(email)) {
                  emails.add(email);
                } else {
                  ProjectLogger.log(
                      "EmailServiceActor:sendMail : Email decryption failed for userId "
                          + user.get(JsonKey.USER_ID));
                }
              } else {
                // if email is blank(or private) then fetch email from cassandra
                userIds.add((String) user.get(JsonKey.USER_ID));
              }
            });
      }
    }
  }

  private String getOrgName(String usrId) {
    String orgName = "";
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

  private Map<String, Object> getUserInfo(List<String> emails) {
    SearchDTO searchDTO = new SearchDTO();
    Map<String, Object> additionalProperties = new HashMap<>();
    additionalProperties.put(
        JsonKey.ENC_EMAIL,
        emails
            .stream()
            .map(
                i -> {
                  String encryptedMail = null;
                  try {
                    encryptedMail = encryptionService.encryptData(i);
                  } catch (Exception e) {
                    ProjectLogger.log(e.getMessage(), e);
                  }
                  return encryptedMail;
                })
            .collect(Collectors.toList()));
    searchDTO.addAdditionalProperty(JsonKey.FILTERS, additionalProperties);
    return ElasticSearchUtil.complexSearch(
        searchDTO, EsIndex.sunbird.getIndexName(), EsType.user.getTypeName());
  }

  private void checkEmailValidity(List<String> emails) {
    for (String email : emails) {
      if (!ProjectUtil.isEmailvalid(email)) {
        ProjectCommonException.throwClientErrorException(ResponseCode.emailFormatError, null);
        return;
      }
    }
  }
}
