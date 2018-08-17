package org.sunbird.learner.actors.notificationservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void sendMail(Request actorMessage) {
    String name = "";
    Map<String, Object> request =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.EMAIL_REQUEST);
    List<String> emails = new ArrayList<>();
    if (null != (List<String>) request.get(JsonKey.RECIPIENT_EMAILS)) {
      emails.addAll((List<String>) request.get(JsonKey.RECIPIENT_EMAILS));
    }

    if (CollectionUtils.isNotEmpty(emails)) {
      checkEmailValidity(emails.toArray(new String[emails.size()]));
    }

    List<String> emailIds = new ArrayList<>(emails);
    List<String> tempUserIdList = new ArrayList<>();
    List<String> userIds = (List<String>) request.get(JsonKey.RECIPIENT_USERIDS);
    if (CollectionUtils.isEmpty(userIds)) {
      userIds = new ArrayList<>();
    }
    getUserEmails(request, emails, userIds);

    if (CollectionUtils.isNotEmpty(userIds)) {
      List<Map<String, Object>> userList = getUserList(userIds);
      if (userIds.size() != userList.size()) {
        Iterator<Map<String, Object>> itr = userList.iterator();

        while (itr.hasNext()) {
          Map<String, Object> map = itr.next();
          tempUserIdList.add((String) map.get(JsonKey.ID));
        }
        for (int i = 0; i < userIds.size(); i++) {
          if (!tempUserIdList.contains(userIds.get(i))) {
            ProjectCommonException.throwClientErrorException(
                ResponseCode.invalidValue, "Invalid userId " + userIds.get(i));
            return;
          }
        }
      } else {
        for (Map<String, Object> map : userList) {
          String email = (String) map.get(JsonKey.EMAIL);
          if (StringUtils.isNotBlank(email)) {
            String decryptedEmail = decryptionService.decryptData(email);
            emailIds.add(decryptedEmail);
          }
          tempUserIdList.add((String) map.get(JsonKey.ID));
          name = (String) map.get(JsonKey.FIRST_NAME);
        }
      }
    }

    // fetch user id om basis of email provided
    if (!emails.isEmpty()) {
      // fetch usr info on basis of email ids
      Map<String, Object> esResult = getUserInfo(emails);
      if (esResult.get(JsonKey.CONTENT) != null
          && !((List) esResult.get(JsonKey.CONTENT)).isEmpty()) {
        List<Map<String, Object>> esSource =
            (List<Map<String, Object>>) esResult.get(JsonKey.CONTENT);
        for (Map<String, Object> m : esSource) {
          tempUserIdList.add((String) m.get(JsonKey.ID));
          name = (String) m.get(JsonKey.FIRST_NAME);
        }
      }
    }

    if (emailIds.size() > 1) {
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
    SendMail.sendMail(
        emailIds.toArray(new String[emailIds.size()]),
        (String) request.get(JsonKey.SUBJECT),
        ProjectUtil.getContext(request),
        ProjectUtil.getTemplate(request),
        getEmailTemplateFile((String) request.get(JsonKey.EMAIL_TEMPLATE_TYPE)));
    Response res = new Response();
    res.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
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

  private static Map<String, Object> elasticComplexSearch(
      Map<String, Object> searchQuery, String index, String type) {
    List<String> fields = new ArrayList<>();
    fields.add(JsonKey.USER_ID);
    fields.add(JsonKey.ENC_EMAIL);
    searchQuery.put(JsonKey.FIELDS, fields);
    return ElasticSearchUtil.complexSearch(
        ElasticSearchUtil.createSearchDTO(searchQuery), index, type);
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

  private void checkEmailValidity(String[] emails) {
    for (String email : emails) {
      if (!ProjectUtil.isEmailvalid(email)) {
        ProjectCommonException.throwClientErrorException(ResponseCode.emailFormatError, null);
        return;
      }
    }
  }
}
