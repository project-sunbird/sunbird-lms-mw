package org.sunbird.learner.actors.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.telemetry.util.TelemetryLmaxWriter;
import org.sunbird.telemetry.util.TelemetryUtil;

/**
 * This class will handle search operation for all different type of index and types
 *
 * @author Manzarul
 */
@ActorConfig(
  tasks = {"compositeSearch"},
  asyncTasks = {}
)
public class SearchHandlerActor extends BaseActor {

  private String topn = PropertiesCache.getInstance().getProperty(JsonKey.SEARCH_TOP_N);

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    // set request id fto thread loacl...
    ExecutionContext.setRequestId(request.getRequestId());

    if (request.getOperation().equalsIgnoreCase(ActorOperations.COMPOSITE_SEARCH.getValue())) {
      Map<String, Object> searchQueryMap = request.getRequest();
      String mentorData = (String) request.get(JsonKey.MENTOR_COURSE_VIEW);
      boolean isMentorViewAllowed = false;
      if (StringUtils.isNotBlank(mentorData)) {
        isMentorViewAllowed = Boolean.parseBoolean(mentorData);
      }
      Object objectType =
          ((Map<String, Object>) searchQueryMap.get(JsonKey.FILTERS)).get(JsonKey.OBJECT_TYPE);
      String[] types = null;
      if (objectType != null && objectType instanceof List) {
        List<String> list = (List) objectType;
        types = list.toArray(new String[list.size()]);
      }
      ((Map<String, Object>) searchQueryMap.get(JsonKey.FILTERS)).remove(JsonKey.OBJECT_TYPE);
      String filterObjectType = "";
      for (String type : types) {
        if (EsType.user.getTypeName().equalsIgnoreCase(type)) {
          filterObjectType = EsType.user.getTypeName();
          UserUtility.encryptUserSearchFilterQueryData(searchQueryMap);
        }
      }
      SearchDTO searchDto = Util.createSearchDto(searchQueryMap);
      if (filterObjectType.equalsIgnoreCase(EsType.user.getTypeName())) {
        searchDto.setExcludedFields(Arrays.asList(ProjectUtil.excludes));
      }
      Map<String, Object> result =
          ElasticSearchUtil.complexSearch(
              searchDto, ProjectUtil.EsIndex.sunbird.getIndexName(), types);
      // now if course mentor view is allowed then do one more elasticsearch
      // based of mentors list.
      if (isMentorViewAllowed) {
        Map<String, Object> mentorCourseBatchResp = getMentorCourseBatch(searchQueryMap);
        result = mergeEsSearchResponse(result, mentorCourseBatchResp);
      }
      // Decrypt the data
      if (EsType.user.getTypeName().equalsIgnoreCase(filterObjectType)) {
        List<Map<String, Object>> userMapList =
            (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
        for (Map<String, Object> userMap : userMapList) {
          UserUtility.decryptUserDataFrmES(userMap);
          userMap.remove(JsonKey.ENC_EMAIL);
          userMap.remove(JsonKey.ENC_PHONE);
        }
      }
      Response response = new Response();
      if (result != null) {
        response.put(JsonKey.RESPONSE, result);
      } else {
        result = new HashMap<>();
        response.put(JsonKey.RESPONSE, result);
      }
      sender().tell(response, self());
      // create search telemetry event here ...
      generateSearchTelemetryEvent(searchDto, types, result);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  private Map<String, Object> getMentorCourseBatch(Map<String, Object> requestedFiltermap) {
    String createdById =
        (String)
            ((Map<String, Object>) requestedFiltermap.get(JsonKey.FILTERS)).get(JsonKey.CREATED_BY);
    ((Map<String, Object>) requestedFiltermap.get(JsonKey.FILTERS)).remove(JsonKey.CREATED_BY);
    List<String> list = new ArrayList<>();
    list.add(createdById);
    ((Map<String, Object>) requestedFiltermap.get(JsonKey.FILTERS)).put(JsonKey.MENTORS, list);
    SearchDTO searchDto = Util.createSearchDto(requestedFiltermap);
    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            searchDto,
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName());
    return result;
  }

  /**
   * @param request1 map will have key as count and content []
   * @param request2 map will have key as count and content []
   * @return map with sum of count and all content.
   */
  private Map<String, Object> mergeEsSearchResponse(
      Map<String, Object> request1, Map<String, Object> request2) {
    long firstReqCount = getCount(request1);
    List<Object> firstReqData = getData(request1);
    long secondReqCount = getCount(request2);
    List<Object> secondReqData = getData(request2);
    long totalCount = firstReqCount + secondReqCount;
    List<Object> list = new ArrayList<>();
    list.addAll(firstReqData);
    list.addAll(secondReqData);
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.COUNT, totalCount);
    map.put(JsonKey.CONTENT, list);
    return map;
  }

  private long getCount(Map<String, Object> request) {

    if (request != null) {
      return (long) request.get(JsonKey.COUNT);
    }
    return 0;
  }

  private List<Object> getData(Map<String, Object> request) {
    if (request != null) {
      return (List<Object>) request.get(JsonKey.CONTENT);
    }
    return new ArrayList<Object>();
  }

  private void generateSearchTelemetryEvent(
      SearchDTO searchDto, String[] types, Map<String, Object> result) {

    Map<String, Object> telemetryContext = TelemetryUtil.getTelemetryContext();

    Map<String, Object> params = new HashMap<>();
    params.put(JsonKey.TYPE, String.join(",", types));
    params.put(JsonKey.QUERY, searchDto.getQuery());
    params.put(JsonKey.FILTERS, searchDto.getAdditionalProperties().get(JsonKey.FILTERS));
    params.put(JsonKey.SORT, searchDto.getSortBy());
    params.put(JsonKey.SIZE, result.get(JsonKey.COUNT));
    params.put(JsonKey.TOPN, generateTopnResult(result)); // need to get topn value from
    // response
    Request req = new Request();
    req.setRequest(telemetryRequestForSearch(telemetryContext, params));
    TelemetryLmaxWriter.getInstance().submitMessage(req);
  }

  private List<Map<String, Object>> generateTopnResult(Map<String, Object> result) {

    List<Map<String, Object>> userMapList = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
    Integer topN = Integer.parseInt(topn);

    List<Map<String, Object>> list = new ArrayList<>();
    if (topN < userMapList.size()) {
      for (int i = 0; i < topN; i++) {
        Map<String, Object> m = new HashMap<>();
        m.put(JsonKey.ID, userMapList.get(i).get(JsonKey.ID));
        list.add(m);
      }
    } else {

      for (int i = 0; i < userMapList.size(); i++) {
        Map<String, Object> m = new HashMap<>();
        m.put(JsonKey.ID, userMapList.get(i).get(JsonKey.ID));
        list.add(m);
      }
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
