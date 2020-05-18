package org.sunbird.location.actors;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.PropertiesCache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/** @author Amit Kumar */
public abstract class BaseLocationActor extends BaseActor {

  private List<Map<String, Object>> generateTopNResult(Map<String, Object> result) {
    List<Map<String, Object>> dataMapList =
        (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    Integer topN =
        Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.SEARCH_TOP_N));
    int count = Math.min(topN, dataMapList.size());
    List<Map<String, Object>> list = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Map<String, Object> m = new HashMap<>();
      m.put(JsonKey.ID, dataMapList.get(i).get(JsonKey.ID));
      list.add(m);
    }
    return list;
  }

  private static Map<String, Object> telemetryRequestForSearch(
      Map<String, Object> telemetryContext, Map<String, Object> params) {
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.CONTEXT, telemetryContext);
    map.put(JsonKey.PARAMS, params);
    map.put(JsonKey.TELEMETRY_EVENT_TYPE, "SEARCH");
    return map;
  }
}
