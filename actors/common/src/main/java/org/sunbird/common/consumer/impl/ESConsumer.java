package org.sunbird.common.consumer.impl;

import java.util.HashMap;
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

  static Map<String, String> identiFierMap = new HashMap<>();
  static Map<String, Boolean> updateMap = new HashMap<>();

  static {
    identiFierMap.put(JsonKey.USER, JsonKey.ID);
    identiFierMap.put(JsonKey.USER_ORG, JsonKey.USER_ID);
    identiFierMap.put("user_skills", JsonKey.USER_ID);

    updateMap.put(JsonKey.USER_ORG, true);
    updateMap.put(JsonKey.SKILLS, true);
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
          eventMessage.getOpertaionType(),
          (String) eventMessage.getMessage().get(identiFierMap.get(eventMessage.getOperationOn())),
          eventMessage.getMessage());
    } else {
      if (updateMap.get(eventMessage.getOperationOn())) {
        Map<String, Object> esMap =
            ElasticSearchUtil.getDataByIdentifier(
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                eventMessage.getOpertaionType(),
                (String)
                    eventMessage
                        .getMessage()
                        .get(identiFierMap.get(eventMessage.getOperationOn())));

      } else {

      }
    }
  }
}
