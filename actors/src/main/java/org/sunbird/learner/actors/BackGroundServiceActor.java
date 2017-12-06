package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

public class BackGroundServiceActor extends UntypedAbstractActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("BackGroundServiceActor onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.UPDATE_USER_COUNT_TO_LOCATIONID.getValue())) {
          updateUserCount(actorMessage);
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
      }
    } else {
      ProjectLogger.log("UNSUPPORTED MESSAGE");
    }
  }

  private void updateUserCount(Request actorMessage) {
    Util.DbInfo locDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
    List<Object> locationIds = (List<Object>) actorMessage.getRequest().get(JsonKey.LOCATION_IDS);
    Response response = cassandraOperation.getRecordsByProperty(locDbInfo.getKeySpace(),
        locDbInfo.getTableName(), JsonKey.ID, locationIds);
    List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (null != list && !list.isEmpty()) {
      for (Map<String, Object> map : list) {
        String locationId = (String) map.get(JsonKey.ID);
        Long userCountTTL = 0L;
        try {
          userCountTTL = Long.valueOf((String) map.get(JsonKey.USER_COUNT_TTL));
        } catch (Exception ex) {
          ProjectLogger.log("Exception occurred while converting string to long "
              + (String) map.get(JsonKey.USER_COUNT_TTL));
        }
        Long currentTime = System.currentTimeMillis();
        Long diff = currentTime - userCountTTL;
        int hours = (int) (diff / (1000 * 60 * 60));
        if (hours >= 24) {
          int userCount = getUserCount(locationId);
          Map<String, Object> reqMap = new HashMap<>();
          reqMap.put(JsonKey.ID, locationId);
          reqMap.put(JsonKey.USER_COUNT, userCount);
          reqMap.put(JsonKey.USER_COUNT_TTL, String.valueOf(System.currentTimeMillis()));
          cassandraOperation.updateRecord(locDbInfo.getKeySpace(), locDbInfo.getTableName(),
              reqMap);
        }
      }
    }
  }

  private static int getUserCount(String locationId) {
    SearchDTO searchDto = new SearchDTO();
    List<String> list = new ArrayList<>();
    list.add(JsonKey.ORGANISATION_ID);
    searchDto.setFields(list);
    Map<String, Object> filter = new HashMap<>();
    filter.put(JsonKey.LOCATION_ID, locationId);
    searchDto.getAdditionalProperties().put(JsonKey.FILTERS, filter);
    Map<String, Object> esResponse = ElasticSearchUtil.complexSearch(searchDto,
        ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.organisation.getTypeName());
    List<Map<String, Object>> orgList = (List<Map<String, Object>>) esResponse.get(JsonKey.CONTENT);

    List<String> orgIdList = new ArrayList<>();
    for (Map<String, Object> map : orgList) {
      orgIdList.add((String) map.get(JsonKey.ORGANISATION_ID));
    }
    searchDto = new SearchDTO();
    List<String> list2 = new ArrayList<>();
    list2.add(JsonKey.USER_ID);
    searchDto.setFields(list);
    searchDto.setLimit(0);
    Map<String, Object> filter2 = new HashMap<>();
    filter2.put(JsonKey.REGISTERED_ORG_ID, orgIdList);
    searchDto.getAdditionalProperties().put(JsonKey.FILTERS, filter2);
    Map<String, Object> esResponse2 = ElasticSearchUtil.complexSearch(searchDto,
        ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.organisation.getTypeName());
    long userCount = (long) esResponse2.get(JsonKey.COUNT);
    return (int) userCount;
  }

}
