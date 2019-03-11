package org.sunbird.common.consumer.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.message.broker.factory.MessageBrokerFactory;
import org.sunbird.common.message.broker.inf.MessageBroker;
import org.sunbird.common.message.broker.model.EventMessage;
import org.sunbird.common.message.consumer.Consumer;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;

/** Created by rajatgupta on 09/03/19. */
public class ESConsumer implements Consumer {

  static Map<String, String> identifierMap = new HashMap<>();
  static Map<String, String> updateMap = new HashMap<>();
  static Map<String, String> uniqueIdMap = new HashMap<>();
  static Map<String, String> topicMap = new HashMap<>();

  static {
    identifierMap.put(JsonKey.USER, JsonKey.ID);
    identifierMap.put(JsonKey.USER_ORG, JsonKey.USER_ID);
    identifierMap.put("user_skills", JsonKey.USER_ID);
    identifierMap.put(JsonKey.USER_COURSES, JsonKey.USER_ID);
    identifierMap.put("user_external_identity", JsonKey.USER_ID);
    // identifierMap.put("user_notes", JsonKey.USER_ID);
    identifierMap.put(JsonKey.EDUCATION_DB, JsonKey.USER_ID);
    identifierMap.put(JsonKey.JOB_PROFILE_DB, JsonKey.USER_ID);
    identifierMap.put(JsonKey.ADDRESS_DB, JsonKey.USER_ID);

    updateMap.put(JsonKey.USER_ORG, JsonKey.ORGANISATION);
    updateMap.put(JsonKey.SKILLS, JsonKey.SKILLS);
    updateMap.put(JsonKey.USER_COURSES, JsonKey.BATCHES);
    updateMap.put(JsonKey.EDUCATION_DB, JsonKey.EDUCATION);
    updateMap.put(JsonKey.ADDRESS_DB, JsonKey.ADDRESS_DB);
    updateMap.put(JsonKey.JOB_PROFILE_DB, JsonKey.JOB_PROFILE);

    uniqueIdMap.put(JsonKey.USER_ORG, JsonKey.ORGANISATION_ID);
    uniqueIdMap.put(JsonKey.USER_COURSES, JsonKey.BATCH_ID);
    uniqueIdMap.put(JsonKey.SKILLS, JsonKey.ID);
    uniqueIdMap.put(JsonKey.EDUCATION_DB, JsonKey.ID);
    uniqueIdMap.put(JsonKey.ADDRESS_DB, JsonKey.ID);
    uniqueIdMap.put(JsonKey.JOB_PROFILE_DB, JsonKey.ID);

    topicMap.put(JsonKey.USER_ORG, JsonKey.USER);
    topicMap.put(JsonKey.USER_COURSES, JsonKey.USER);
    topicMap.put(JsonKey.SKILLS, JsonKey.USER);
    topicMap.put(JsonKey.EDUCATION_DB, JsonKey.USER);
    topicMap.put(JsonKey.ADDRESS_DB, JsonKey.USER);
    topicMap.put(JsonKey.JOB_PROFILE_DB, JsonKey.USER);
  }

  @Override
  public void consume() {
    ProjectLogger.log("ESConsumer:execute: ESConsumer job trigerred.", LoggerEnum.INFO);

    performJob();
  }

  private void performJob() {
    ProjectLogger.log("ESConsumer:performJob: ESConsumer  performJob.");
    MessageBroker messageBroker = MessageBrokerFactory.getInstance();
    EventMessage eventMessage = messageBroker.recieve(JsonKey.DATABASE_OPERATION);

    if (JsonKey.INSERT.equalsIgnoreCase(eventMessage.getOperationType())) {
      ElasticSearchUtil.upsertData(
          ProjectUtil.EsIndex.sunbird.getIndexName(),
          topicMap.get(eventMessage.getOpertaionType()),
          (String) eventMessage.getMessage().get(identifierMap.get(eventMessage.getOperationOn())),
          eventMessage.getMessage());
    } else {
      updateData(eventMessage);
      /*
       * if (updateMap.get(eventMessage.getOperationOn())) { Map<String, Object> esMap
       * = ElasticSearchUtil.getDataByIdentifier(
       * ProjectUtil.EsIndex.sunbird.getIndexName(), eventMessage.getOpertaionType(),
       * (String) eventMessage .getMessage()
       * .get(identifierMap.get(eventMessage.getOperationOn())));
       *
       * }
       */
    }
  }

  private void updateData(EventMessage eventMessage) {
    Map<String, Object> esMap =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            eventMessage.getOpertaionType(),
            (String)
                eventMessage.getMessage().get(identifierMap.get(eventMessage.getOperationOn())));
    Object obj = esMap.get(updateMap.get(eventMessage.getOperationOn()));
    if (obj == null) {
      esMap.put(updateMap.get(eventMessage.getOperationOn()), eventMessage.getMessage());
    } else if (obj instanceof Collection) {
      if (((Collection) obj).isEmpty()) {
        ((Collection) obj).add(eventMessage.getMessage());
      } else {
        boolean contains = false;
        for (Map<String, Object> map : (List<Map>) obj) {
          if (map.get(uniqueIdMap.get(eventMessage.getOperationOn()))
              .equals(eventMessage.getMessage().get(JsonKey.ID))) {
            map.putAll(eventMessage.getMessage());
            contains = true;
          }
        }
        if (!contains) {
          ((Collection) obj).add(eventMessage.getMessage());
        }
      }
    }
    ElasticSearchUtil.upsertData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        topicMap.get(eventMessage.getOpertaionType()),
        (String) eventMessage.getMessage().get(identifierMap.get(eventMessage.getOperationOn())),
        eventMessage.getMessage());
  }
}
