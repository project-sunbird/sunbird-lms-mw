package org.sunbird.learner.actors.notificationservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.sunbird.actor.background.BackgroundOperations;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
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
	  ProjectLogger.log("EmailServiceActor received action: " + request.getOperation(), LoggerEnum.INFO.name());
    if (request.getOperation().equalsIgnoreCase(BackgroundOperations.emailService.name())) {
//      sendMail(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void sendMail(Request actorMessage) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    String name = "";
    Map<String, Object> request =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.EMAIL_REQUEST);
    List<String> emails = (List<String>) request.get(JsonKey.RECIPIENT_EMAILS);
    if (null == emails) {
      emails = new ArrayList<>();
    }
    checkEmailValidity(emails.toArray(new String[emails.size()]));
    List<String> emailIds = new ArrayList<>(emails);
    List<String> tempUserIdList = new ArrayList<>();

    if (null != request.get(JsonKey.RECIPIENT_USERIDS)) {
      List<String> userIds = (List<String>) request.get(JsonKey.RECIPIENT_USERIDS);
      if (!userIds.isEmpty()) {
        Response response =
            cassandraOperation.getRecordsByProperty(
                usrDbInfo.getKeySpace(),
                usrDbInfo.getTableName(),
                JsonKey.ID,
                new ArrayList<>(userIds));
        List<Map<String, Object>> respMapList =
            (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        if (userIds.size() != respMapList.size()) {
          Iterator<Map<String, Object>> itr = respMapList.iterator();

          while (itr.hasNext()) {
            Map<String, Object> map = itr.next();
            tempUserIdList.add((String) map.get(JsonKey.ID));
          }
          for (int i = 0; i < userIds.size(); i++) {
            if (!tempUserIdList.contains((String) userIds.get(i))) {
              Response resp = new Response();
              resp.put((String) userIds.get(i), "Invalid UserId.");
              sender().tell(resp, self());
              return;
            }
          }
        } else {
          for (Map<String, Object> map : respMapList) {
            String decryptedEmail = decryptionService.decryptData((String) map.get(JsonKey.EMAIL));
            emailIds.add(decryptedEmail);
            tempUserIdList.add((String) map.get(JsonKey.ID));
            name = (String) map.get(JsonKey.FIRST_NAME);
          }
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
        ProjectUtil.getTemplate(request));
    Response res = new Response();
    res.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(res, self());
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

  private void checkEmailValidity(String[] emails) {
    for (String email : emails) {
      if (!ProjectUtil.isEmailvalid(email)) {
        Response response = new Response();
        response.put(email, ResponseCode.emailFormatError.getErrorMessage());
        sender().tell(response, self());
        return;
      }
    }
  }
}
