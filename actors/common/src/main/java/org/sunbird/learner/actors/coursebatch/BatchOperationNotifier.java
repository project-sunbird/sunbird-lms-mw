package org.sunbird.learner.actors.coursebatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.notificationservice.EmailServiceActor;
import org.sunbird.models.course.batch.CourseBatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * This actor will handle course enrollment operation .
 *
 * @author github.com/iostream04
 */

public class BatchOperationNotifier {

  private EmailServiceActor emailServiceActor;

  private static String URL = "https://dev.open-sunbird.org/api/data/v1/form/read";

  private static String bearerString = "Bearer";

  private static String AUTH_KEY = PropertiesCache.getInstance().getProperty(JsonKey.ACCESS_TOKEN);

  public String MENTOR = "UserTypeMentor";
  public String PARTICIPANTS = "UserTypeParticipants";

  // from operation type we can decide the type of mail we have to get i.e
  // enrolled / unenrolled
  public void batchOperationNotifier(Map<String, Object> courseMap, String operationType) {
    Request request = getMailRequest(courseMap);
    sendMail(request);
  }

  public void batchBulkOperationNotifier(CourseBatch courseBatch, String userType, String operationType) {

    if (userType.equals(MENTOR)) {
      if (operationType.equals(JsonKey.ADD)) {
        Request requestForMentor = getMailRequest(courseBatch, MENTOR, JsonKey.ADD);
        sendMail(requestForMentor);
      } else if (operationType.equals(JsonKey.REMOVE)) {
        Request requestForMentor = getMailRequest(courseBatch, MENTOR, JsonKey.ADD);
        sendMail(requestForMentor);
      }
    }
    if (userType.equals(PARTICIPANTS)) {

      if (operationType.equals(JsonKey.ADD)) {
        Request requestForParticipients = getMailRequest(courseBatch, PARTICIPANTS, JsonKey.ADD);
        sendMail(requestForParticipients);
      } else if (operationType.equals(JsonKey.REMOVE)) {
        Request requestForParticipients = getMailRequest(courseBatch, PARTICIPANTS, JsonKey.REMOVE);
        sendMail(requestForParticipients);
      }

    }

  }

  public void batchUpdateOperationNotifier(CourseBatch courseBatchPrev, CourseBatch courseBatchNew) {
    List<String> prevMentors = courseBatchPrev.getMentors();
    List<String> newMentors = courseBatchNew.getMentors();
    List<String> removedMentors = courseBatchPrev.getMentors();
    if (prevMentors == null) {
      prevMentors = new ArrayList<>();
      removedMentors = new ArrayList<>();
    }
    if (newMentors == null) {
      newMentors = new ArrayList<>();
    }
    for (String mentor : prevMentors) {
      if (newMentors.contains(mentor)) {
        newMentors.remove(mentor);
        removedMentors.remove(mentor);
      }
    }
    courseBatchPrev.setMentors(removedMentors);
    courseBatchNew.setMentors(newMentors);
    if (!removedMentors.isEmpty())
      batchBulkOperationNotifier(courseBatchPrev, MENTOR, JsonKey.REMOVE);
    if (!newMentors.isEmpty())
      batchBulkOperationNotifier(courseBatchNew, MENTOR, JsonKey.ADD);
    Map<String, Boolean> oldParticipants = courseBatchPrev.getParticipant();
    Map<String, Boolean> newParticipants = courseBatchNew.getParticipant();
    Map<String, Boolean> removedParticipants = courseBatchPrev.getParticipant();
    if (oldParticipants == null) {
      oldParticipants = new HashMap<>();
      removedParticipants = new HashMap<>();
    }
    if (newParticipants == null)
      newParticipants = new HashMap<>();
    Set<String> oldParticipantsSet = oldParticipants.keySet();
    for (String participant : oldParticipantsSet) {
      if (newParticipants.containsKey(participant)) {
        newParticipants.remove(participant);
        removedParticipants.remove(participant);
      }
    }
    courseBatchPrev.setParticipant(removedParticipants);
    courseBatchNew.setParticipant(newParticipants);
    batchBulkOperationNotifier(courseBatchPrev, PARTICIPANTS, JsonKey.REMOVE);
    batchBulkOperationNotifier(courseBatchNew, PARTICIPANTS, JsonKey.ADD);
  }

  private void sendMail(Request request) {

    try {
      emailServiceActor.onReceive(request);
    } catch (Throwable e) {
      ProjectLogger.log("Error occured during sending mail for batch course process " + e, LoggerEnum.ERROR);
    }

  }

  private Request getMailRequest(Map<String, Object> courseMap) {
    Request request = new Request();
    Map<String, Object> requestMap = getRequestMap(courseMap);
    request.getRequest().put(JsonKey.EMAIL_REQUEST, requestMap);
    return request;
  }

  private Request getMailRequest(CourseBatch courseBatch, String userType, String operationType) {

    Request request = new Request();
    Map<String, Object> requestMap = getRequestMap(courseBatch, userType, operationType);
    request.getRequest().put(JsonKey.EMAIL_REQUEST, requestMap);
    return request;
  }

  private Map<String, Object> getRequestMap(Map<String, Object> courseMap) {
    Map<String, Object> requestMap = new HashMap<>();
    List<String> users = new ArrayList<>();
    users.add((String) courseMap.get(JsonKey.USER_ID));

    requestMap.put(JsonKey.RECIPIENT_USERIDS, users);
    requestMap.put(JsonKey.EMAIL_TEMPLATE_TYPE, null);
    requestMap.put(JsonKey.SUBJECT, courseMap.get(JsonKey.SUBJECT));
    requestMap.put(JsonKey.ORG_NAME, courseMap.get(JsonKey.ORG_NAME));
    requestMap.put(JsonKey.ORG_IMAGE_URL, courseMap.get(JsonKey.ORG_IMAGE_URL));
    requestMap.put(JsonKey.COURSE_LOGO_URL, courseMap.get(JsonKey.COURSE_LOGO_URL));
    requestMap.put(JsonKey.COURSE_NAME, courseMap.get(JsonKey.COURSE_NAME));
    requestMap.put(JsonKey.DESCRIPTION, courseMap.get(JsonKey.DESCRIPTION));
    requestMap.put(JsonKey.ADDED_BY, courseMap.get(JsonKey.ADDED_BY));

    return requestMap;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getRequestMap(CourseBatch courseBatch, String userType, String operationType) {
    Map<String, Object> courseBatchObject = new ObjectMapper().convertValue(courseBatch, Map.class);
    Map<String, String> additionalCourseInfo = (Map<String, String>) courseBatchObject
        .get(JsonKey.COURSE_ADDITIONAL_INFO);

    Map<String, Object> requestMap = new HashMap<>();
    if (userType.equals(MENTOR)) {
      getMentors(requestMap, courseBatch);
    }
    if (userType.equals(PARTICIPANTS)) {
      getParticipants(requestMap, courseBatch);
    }
    // template can be set depending upon operationType
    requestMap.put(JsonKey.EMAIL_TEMPLATE_TYPE, null);
    // setting description as subject
    requestMap.put(JsonKey.SUBJECT, courseBatch.getCourseAdditionalInfo().get(JsonKey.SUBJECT));
    requestMap.put(JsonKey.ORG_NAME, "");
    // we have to create context which required following Name (
    // ???),Body,fromEmail
    // Org_name,Org_Image_uRl,ActionName, UserName, temporary_passworf
    // Action_URL
    requestMap.put(JsonKey.COURSE_LOGO_URL, additionalCourseInfo.get(JsonKey.COURSE_LOGO_URL));
    requestMap.put(JsonKey.COURSE_NAME, additionalCourseInfo.get(JsonKey.COURSE_NAME));
    requestMap.put(JsonKey.DESCRIPTION, additionalCourseInfo.get(JsonKey.DESCRIPTION));

    return requestMap;
  }

  private void getParticipants(Map<String, Object> requestMap, CourseBatch courseBatch) {

    List<String> users = new ArrayList<>();
    Map<String, Boolean> participants = courseBatch.getParticipant();
    Set<String> key = participants.keySet();
    for (String user : key) {
      if (participants.get(user)) {
        users.add(user);
      }
    }
    requestMap.put(JsonKey.RECIPIENT_EMAILS, users);

  }

  private void getMentors(Map<String, Object> requestMap, CourseBatch courseBatch) {

    requestMap.put(JsonKey.RECIPIENT_EMAILS, courseBatch.getMentors());
  }

  private String getTemplateFromFormApi(CourseBatch courseBatch) {

    HttpClient client = HttpClientBuilder.create().build();
    HttpPost request = new HttpPost(URL);
    request.setHeader("content-type", "application/json");
    request.setHeader("Authorization", bearerString + " " + AUTH_KEY);

    JsonObject requestObject = new JsonObject();
    /*
     * requestObject.addProperty("type", (String) reqObj.get("type"));
     * requestObject.addProperty("subType", (String) reqObj.get("subType"));
     * requestObject.addProperty("action", (String) reqObj.get("action"));
     * requestObject.addProperty("framework", (String) reqObj.get("framework"));
     */requestObject.addProperty("rootOrgId", getRootOrg(courseBatch.getCourseCreator()));
    JsonObject requestBody = new JsonObject();
    requestBody.add("request", requestObject);

    try {
      StringEntity entityString = new StringEntity(requestBody.toString());
      request.setEntity(entityString);

      HttpResponse response = client.execute(request);
      HttpEntity entity = response.getEntity();

      if (entity != null) {
        return EntityUtils.toString(entity);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  private Map<String, String> getTemplateData(String apiResponse) {
    Map<String, String> templateData = new HashMap<>();
    JsonParser parser = new JsonParser();
    JsonObject json = (JsonObject) parser.parse(apiResponse);

    JsonObject result = json.getAsJsonObject("result");
    JsonObject form = result.getAsJsonObject("form");
    JsonObject data = form.getAsJsonObject("data");
    JsonArray fields = data.getAsJsonArray("fields");
    JsonElement values = fields.get(0);
    JsonObject value = values.getAsJsonObject();
    templateData.put("templateName", data.get("templateName").getAsString());
    templateData.put("body", value.get("body").getAsString());
    templateData.put("subject", value.get("subject").getAsString());
    templateData.put("logo", value.get("logo").getAsString());
    templateData.put("orgName", value.get("orgName").getAsString());
    templateData.put("fromEmail", value.get("fromEmail").getAsString());
    return templateData;
  }

  private String getRootOrg(String batchCreator) {
    Map<String, Object> userInfo = ElasticSearchUtil.getDataByIdentifier(EsIndex.sunbird.getIndexName(),
        EsType.user.getTypeName(), batchCreator);
    return getRootOrgFromUserMap(userInfo);
  }

  @SuppressWarnings("unchecked")
  private String getRootOrgFromUserMap(Map<String, Object> userInfo) {

    String rootOrg = (String) userInfo.get(JsonKey.ROOT_ORG_ID);
    Map<String, Object> registeredOrgInfo = (Map<String, Object>) userInfo.get(JsonKey.REGISTERED_ORG);
    if (registeredOrgInfo != null && !registeredOrgInfo.isEmpty()) {
      if (null != registeredOrgInfo.get(JsonKey.IS_ROOT_ORG) && (Boolean) registeredOrgInfo.get(JsonKey.IS_ROOT_ORG)) {
        rootOrg = (String) registeredOrgInfo.get(JsonKey.ID);
      }
    }
    return rootOrg;
  }

}
