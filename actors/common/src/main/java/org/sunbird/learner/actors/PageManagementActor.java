package org.sunbird.learner.actors;

import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.pattern.Patterns;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.ContentSearchUtil;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;
import org.sunbird.telemetry.util.TelemetryUtil;
import scala.concurrent.Future;

/**
 * This actor will handle page management operation .
 *
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {
    "createPage",
    "updatePage",
    "getPageData",
    "getPageSettings",
    "getPageSetting",
    "createSection",
    "updateSection",
    "getSection",
    "getAllSection"
  },
  asyncTasks = {}
)
public class PageManagementActor extends BaseActor {

  private Util.DbInfo pageDbInfo = Util.dbInfoMap.get(JsonKey.PAGE_MGMT_DB);
  private Util.DbInfo sectionDbInfo = Util.dbInfoMap.get(JsonKey.SECTION_MGMT_DB);
  private Util.DbInfo pageSectionDbInfo = Util.dbInfoMap.get(JsonKey.PAGE_SECTION_DB);
  private Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.PAGE);

    ExecutionContext.setRequestId(request.getRequestId());
    if (request.getOperation().equalsIgnoreCase(ActorOperations.CREATE_PAGE.getValue())) {
      createPage(request);
    } else if (request.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_PAGE.getValue())) {
      updatePage(request);
    } else if (request
        .getOperation()
        .equalsIgnoreCase(ActorOperations.GET_PAGE_SETTING.getValue())) {
      getPageSetting(request);
    } else if (request
        .getOperation()
        .equalsIgnoreCase(ActorOperations.GET_PAGE_SETTINGS.getValue())) {
      getPageSettings();
    } else if (request.getOperation().equalsIgnoreCase(ActorOperations.GET_PAGE_DATA.getValue())) {
      getPageData(request);
    } else if (request.getOperation().equalsIgnoreCase(ActorOperations.CREATE_SECTION.getValue())) {
      createPageSection(request);
    } else if (request.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_SECTION.getValue())) {
      updatePageSection(request);
    } else if (request.getOperation().equalsIgnoreCase(ActorOperations.GET_SECTION.getValue())) {
      getSection(request);
    } else if (request
        .getOperation()
        .equalsIgnoreCase(ActorOperations.GET_ALL_SECTION.getValue())) {
      getAllSections();
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  private void getAllSections() {
    Response response = null;
    response =
        cassandraOperation.getAllRecords(sectionDbInfo.getKeySpace(), sectionDbInfo.getTableName());
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    for (Map<String, Object> map : result) {
      removeUnwantedData(map, "");
    }
    Response sectionMap = new Response();
    sectionMap.put(JsonKey.SECTIONS, response.get(JsonKey.RESPONSE));
    sender().tell(response, self());
  }

  private void getSection(Request actorMessage) {
    Response response = null;
    Map<String, Object> req = actorMessage.getRequest();
    String sectionId = (String) req.get(JsonKey.ID);
    response =
        cassandraOperation.getRecordById(
            sectionDbInfo.getKeySpace(), sectionDbInfo.getTableName(), sectionId);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    if (!(result.isEmpty())) {
      Map<String, Object> map = result.get(0);
      removeUnwantedData(map, "");
      Response section = new Response();
      section.put(JsonKey.SECTION, response.get(JsonKey.RESPONSE));
    }
    sender().tell(response, self());
  }

  private void updatePageSection(Request actorMessage) {
    ProjectLogger.log("Inside updatePageSection method", LoggerEnum.INFO);
    Map<String, Object> req = actorMessage.getRequest();
    // object of telemetry event...
    Map<String, Object> targetObject = new HashMap<>();
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    @SuppressWarnings("unchecked")
    Map<String, Object> sectionMap = (Map<String, Object>) req.get(JsonKey.SECTION);
    if (null != sectionMap.get(JsonKey.SEARCH_QUERY)) {
      try {
        sectionMap.put(
            JsonKey.SEARCH_QUERY, mapper.writeValueAsString(sectionMap.get(JsonKey.SEARCH_QUERY)));
      } catch (IOException e) {
        ProjectLogger.log("Exception occurred while processing search query " + e.getMessage(), e);
      }
    }
    if (null != sectionMap.get(JsonKey.SECTION_DISPLAY)) {
      try {
        sectionMap.put(
            JsonKey.SECTION_DISPLAY,
            mapper.writeValueAsString(sectionMap.get(JsonKey.SECTION_DISPLAY)));
      } catch (IOException e) {
        ProjectLogger.log("Exception occurred while processing display " + e.getMessage(), e);
      }
    }
    sectionMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    ProjectLogger.log("update section details", LoggerEnum.INFO);
    Response response =
        cassandraOperation.updateRecord(
            sectionDbInfo.getKeySpace(), sectionDbInfo.getTableName(), sectionMap);
    sender().tell(response, self());
    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) sectionMap.get(JsonKey.ID), JsonKey.PAGE_SECTION, JsonKey.CREATE, null);
    TelemetryUtil.telemetryProcessingCall(
        actorMessage.getRequest(), targetObject, correlatedObject);
    // update DataCacheHandler section map with updated page section data
    ProjectLogger.log("Calling  updateSectionDataCache method", LoggerEnum.INFO);
    updateSectionDataCache(response, sectionMap);
  }

  private void createPageSection(Request actorMessage) {
    ProjectLogger.log("Inside createPageSection method", LoggerEnum.INFO);
    Map<String, Object> req = actorMessage.getRequest();
    @SuppressWarnings("unchecked")
    Map<String, Object> sectionMap = (Map<String, Object>) req.get(JsonKey.SECTION);
    // object of telemetry event...
    Map<String, Object> targetObject = new HashMap<>();
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    if (null != sectionMap.get(JsonKey.SEARCH_QUERY)) {
      try {
        sectionMap.put(
            JsonKey.SEARCH_QUERY, mapper.writeValueAsString(sectionMap.get(JsonKey.SEARCH_QUERY)));
      } catch (IOException e) {
        ProjectLogger.log("Exception occurred while processing search Query " + e.getMessage(), e);
      }
    }
    if (null != sectionMap.get(JsonKey.SECTION_DISPLAY)) {
      try {
        sectionMap.put(
            JsonKey.SECTION_DISPLAY,
            mapper.writeValueAsString(sectionMap.get(JsonKey.SECTION_DISPLAY)));
      } catch (IOException e) {
        ProjectLogger.log("Exception occurred while processing Section display", e);
      }
    }
    sectionMap.put(JsonKey.ID, uniqueId);
    sectionMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
    sectionMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    Response response =
        cassandraOperation.insertRecord(
            sectionDbInfo.getKeySpace(), sectionDbInfo.getTableName(), sectionMap);
    response.put(JsonKey.SECTION_ID, uniqueId);
    sender().tell(response, self());
    targetObject =
        TelemetryUtil.generateTargetObject(uniqueId, JsonKey.PAGE_SECTION, JsonKey.CREATE, null);
    TelemetryUtil.telemetryProcessingCall(
        actorMessage.getRequest(), targetObject, correlatedObject);
    // update DataCacheHandler section map with new page section data
    ProjectLogger.log("Calling  updateSectionDataCache method", LoggerEnum.INFO);
    updateSectionDataCache(response, sectionMap);
  }

  private void updateSectionDataCache(Response response, Map<String, Object> sectionMap) {
    new Thread(
            () -> {
              if ((JsonKey.SUCCESS).equalsIgnoreCase((String) response.get(JsonKey.RESPONSE))) {
                DataCacheHandler.getSectionMap()
                    .put((String) sectionMap.get(JsonKey.ID), sectionMap);
              }
            })
        .start();
  }

  @SuppressWarnings("unchecked")
  private void getPageData(Request actorMessage) throws Exception {
    ProjectLogger.log("Inside getPageData method", LoggerEnum.INFO);
    String sectionQuery = null;
    Map<String, Object> filterMap = new HashMap<>();
    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.PAGE);
    String pageName = (String) req.get(JsonKey.PAGE_NAME);
    String source = (String) req.get(JsonKey.SOURCE);
    String orgId = (String) req.get(JsonKey.ORGANISATION_ID);
    String urlQueryString = (String) actorMessage.getContext().get(JsonKey.URL_QUERY_STRING);
    Map<String, String> headers =
        (Map<String, String>) actorMessage.getRequest().get(JsonKey.HEADER);
    filterMap.putAll(req);
    filterMap.remove(JsonKey.PAGE_NAME);
    filterMap.remove(JsonKey.SOURCE);
    filterMap.remove(JsonKey.ORG_CODE);
    filterMap.remove(JsonKey.FILTERS);
    filterMap.remove(JsonKey.CREATED_BY);
    Map<String, Object> reqFilters = (Map<String, Object>) req.get(JsonKey.FILTERS);

    /** if orgId is not then consider default page */
    if (StringUtils.isBlank(orgId)) {
      orgId = "NA";
    }
    ProjectLogger.log("Fetching data from Cache for " + orgId + ":" + pageName, LoggerEnum.INFO);
    Map<String, Object> pageMap = DataCacheHandler.getPageMap().get(orgId + ":" + pageName);
    if (null == pageMap) {
      throw new ProjectCommonException(
          ResponseCode.pageDoesNotExist.getErrorCode(),
          ResponseCode.pageDoesNotExist.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    if (source.equalsIgnoreCase(ProjectUtil.Source.WEB.getValue())) {
      if (null != pageMap && null != pageMap.get(JsonKey.PORTAL_MAP)) {
        sectionQuery = (String) pageMap.get(JsonKey.PORTAL_MAP);
      }
    } else {
      if (null != pageMap && null != pageMap.get(JsonKey.APP_MAP)) {
        sectionQuery = (String) pageMap.get(JsonKey.APP_MAP);
      }
    }
    Object[] arr = null;
    try {
      ProjectLogger.log(
          "PageManagementActor:getPageData: section query = " + sectionQuery, LoggerEnum.INFO);
      arr = mapper.readValue(sectionQuery, Object[].class);
    } catch (Exception e) {
      ProjectLogger.log(
          "PageManagementActor:getPageData: Exception occurred with error message =  "
              + e.getMessage(),
          LoggerEnum.INFO);
      throw new ProjectCommonException(
          ResponseCode.errorInvalidPageSection.getErrorCode(),
          ResponseCode.errorInvalidPageSection.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    try {
      List<Future<Map<String, Object>>> sectionList = new ArrayList<>();
      if (arr != null) {
        for (Object obj : arr) {
          Map<String, Object> sectionMap = (Map<String, Object>) obj;
          Map<String, Object> sectionData =
                  new HashMap<>(DataCacheHandler.getSectionMap().get(sectionMap.get(JsonKey.ID)));
          Future<Map<String, Object>> contentFuture =
                  getContentData(
                          sectionData,
                          reqFilters,
                          headers,
                          filterMap,
                          urlQueryString,
                          sectionMap.get(JsonKey.GROUP),
                          sectionMap.get(JsonKey.INDEX));
          sectionList.add(contentFuture);
        }
      }

      Future<Iterable<Map<String, Object>>> sctionsFuture =
              Futures.sequence(sectionList, getContext().dispatcher());
      Future<Response> response =
              sctionsFuture.map(
                      new Mapper<Iterable<Map<String, Object>>, Response>() {
                        @Override
                        public Response apply(Iterable<Map<String, Object>> sections) {
                          ProjectLogger.log(
                                  "PageManagementActor:getPageData: apply called ", LoggerEnum.INFO);
                          ArrayList<Map<String, Object>> sectionList = Lists.newArrayList(sections);
                          Map<String, Object> result = new HashMap<>();
                          result.put(JsonKey.NAME, pageMap.get(JsonKey.NAME));
                          result.put(JsonKey.ID, pageMap.get(JsonKey.ID));
                          result.put(JsonKey.SECTIONS, sectionList);
                          Response response = new Response();
                          response.put(JsonKey.RESPONSE, result);
                          ProjectLogger.log(
                                  "PageManagementActor:getPageData: response = " + response, LoggerEnum.INFO);
                          return response;
                        }
                      },
                      getContext().dispatcher());
      Patterns.pipe(response, getContext().dispatcher()).to(sender());
    } catch (Exception e) {
      ProjectLogger.log(
              "PageManagementActor:getPageData: Exception occurred with error message = " + e.getMessage(), LoggerEnum.INFO);
      ProjectLogger.log(
              "PageManagementActor:getPageData: Exception occurred with error message = " + e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  private void getPageSetting(Request actorMessage) {
    Map<String, Object> req = actorMessage.getRequest();
    String pageName = (String) req.get(JsonKey.ID);
    Response response =
        cassandraOperation.getRecordsByProperty(
            pageDbInfo.getKeySpace(), pageDbInfo.getTableName(), JsonKey.PAGE_NAME, pageName);
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    if (!(result.isEmpty())) {
      Map<String, Object> pageDO = result.get(0);
      Map<String, Object> responseMap = getPageSetting(pageDO);
      response.getResult().put(JsonKey.PAGE, responseMap);
      response.getResult().remove(JsonKey.RESPONSE);
    }
    sender().tell(response, self());
  }

  @SuppressWarnings("unchecked")
  private void getPageSettings() {
    Response response =
        cassandraOperation.getAllRecords(pageDbInfo.getKeySpace(), pageDbInfo.getTableName());
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    List<Map<String, Object>> pageList = new ArrayList<>();
    for (Map<String, Object> pageDO : result) {
      Map<String, Object> responseMap = getPageSetting(pageDO);
      pageList.add(responseMap);
    }
    response.getResult().put(JsonKey.PAGE, pageList);
    response.getResult().remove(JsonKey.RESPONSE);
    sender().tell(response, self());
  }

  @SuppressWarnings("unchecked")
  private void updatePage(Request actorMessage) {
    ProjectLogger.log("Inside updatePage method", LoggerEnum.INFO);
    Map<String, Object> req = actorMessage.getRequest();
    Map<String, Object> pageMap = (Map<String, Object>) req.get(JsonKey.PAGE);
    // object of telemetry event...
    Map<String, Object> targetObject = new HashMap<>();
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    // default value for orgId
    if (StringUtils.isBlank((String) pageMap.get(JsonKey.ORGANISATION_ID))) {
      pageMap.put(JsonKey.ORGANISATION_ID, "NA");
    }
    if (!StringUtils.isBlank((String) pageMap.get(JsonKey.PAGE_NAME))) {
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.PAGE_NAME, pageMap.get(JsonKey.PAGE_NAME));
      map.put(JsonKey.ORGANISATION_ID, pageMap.get(JsonKey.ORGANISATION_ID));

      Response res =
          cassandraOperation.getRecordsByProperties(
              pageDbInfo.getKeySpace(), pageDbInfo.getTableName(), map);
      if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
        Map<String, Object> page = ((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).get(0);
        if (!(((String) page.get(JsonKey.ID)).equals(pageMap.get(JsonKey.ID)))) {
          ProjectCommonException exception =
              new ProjectCommonException(
                  ResponseCode.pageAlreadyExist.getErrorCode(),
                  ResponseCode.pageAlreadyExist.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
          return;
        }
      }
    }
    pageMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    if (null != pageMap.get(JsonKey.PORTAL_MAP)) {
      try {
        pageMap.put(JsonKey.PORTAL_MAP, mapper.writeValueAsString(pageMap.get(JsonKey.PORTAL_MAP)));
      } catch (IOException e) {
        ProjectLogger.log("Exception occurred while updating portal map data " + e.getMessage(), e);
      }
    }
    if (null != pageMap.get(JsonKey.APP_MAP)) {
      try {
        pageMap.put(JsonKey.APP_MAP, mapper.writeValueAsString(pageMap.get(JsonKey.APP_MAP)));
      } catch (IOException e) {
        ProjectLogger.log("Exception occurred while updating app map data " + e.getMessage(), e);
      }
    }
    Response response =
        cassandraOperation.updateRecord(
            pageDbInfo.getKeySpace(), pageDbInfo.getTableName(), pageMap);
    sender().tell(response, self());

    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) pageMap.get(JsonKey.ID), JsonKey.PAGE, JsonKey.CREATE, null);
    TelemetryUtil.telemetryProcessingCall(
        actorMessage.getRequest(), targetObject, correlatedObject);
    // update DataCacheHandler page map with updated page data
    ProjectLogger.log(
        "Calling updatePageDataCacheHandler while updating page data ", LoggerEnum.INFO);
    updatePageDataCacheHandler(response, pageMap);
  }

  @SuppressWarnings("unchecked")
  private void createPage(Request actorMessage) {
    Map<String, Object> req = actorMessage.getRequest();
    Map<String, Object> pageMap = (Map<String, Object>) req.get(JsonKey.PAGE);
    // object of telemetry event...
    Map<String, Object> targetObject = new HashMap<>();
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    // default value for orgId
    String orgId = (String) pageMap.get(JsonKey.ORGANISATION_ID);
    if (StringUtils.isNotBlank(orgId)) {
      validateOrg(orgId);
    } else {
      pageMap.put(JsonKey.ORGANISATION_ID, "NA");
    }
    String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    if (!StringUtils.isBlank((String) pageMap.get(JsonKey.PAGE_NAME))) {
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.PAGE_NAME, pageMap.get(JsonKey.PAGE_NAME));
      map.put(JsonKey.ORGANISATION_ID, pageMap.get(JsonKey.ORGANISATION_ID));

      Response res =
          cassandraOperation.getRecordsByProperties(
              pageDbInfo.getKeySpace(), pageDbInfo.getTableName(), map);
      if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
        ProjectCommonException exception =
            new ProjectCommonException(
                ResponseCode.pageAlreadyExist.getErrorCode(),
                ResponseCode.pageAlreadyExist.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }
    }
    pageMap.put(JsonKey.ID, uniqueId);
    pageMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    if (null != pageMap.get(JsonKey.PORTAL_MAP)) {
      try {
        pageMap.put(JsonKey.PORTAL_MAP, mapper.writeValueAsString(pageMap.get(JsonKey.PORTAL_MAP)));
      } catch (IOException e) {
        ProjectLogger.log(e.getMessage(), e);
      }
    }
    if (null != pageMap.get(JsonKey.APP_MAP)) {
      try {
        pageMap.put(JsonKey.APP_MAP, mapper.writeValueAsString(pageMap.get(JsonKey.APP_MAP)));
      } catch (IOException e) {
        ProjectLogger.log(e.getMessage(), e);
      }
    }
    Response response =
        cassandraOperation.insertRecord(
            pageDbInfo.getKeySpace(), pageDbInfo.getTableName(), pageMap);
    response.put(JsonKey.PAGE_ID, uniqueId);
    sender().tell(response, self());
    targetObject = TelemetryUtil.generateTargetObject(uniqueId, JsonKey.PAGE, JsonKey.CREATE, null);
    TelemetryUtil.telemetryProcessingCall(
        actorMessage.getRequest(), targetObject, correlatedObject);

    updatePageDataCacheHandler(response, pageMap);
  }

  private void updatePageDataCacheHandler(Response response, Map<String, Object> pageMap) {
    // update DataCacheHandler page map with new page data
    new Thread(
            () -> {
              if (JsonKey.SUCCESS.equalsIgnoreCase((String) response.get(JsonKey.RESPONSE))) {
                String orgId = "NA";
                if (pageMap.containsKey(JsonKey.ORGANISATION_ID)) {
                  orgId = (String) pageMap.get(JsonKey.ORGANISATION_ID);
                }
                DataCacheHandler.getPageMap()
                    .put(orgId + ":" + (String) pageMap.get(JsonKey.PAGE_NAME), pageMap);
              }
            })
        .start();
  }

  @SuppressWarnings("unchecked")
  private Future<Map<String, Object>> getContentData(
      Map<String, Object> section,
      Map<String, Object> reqFilters,
      Map<String, String> headers,
      Map<String, Object> filterMap,
      String urlQueryString,
      Object group,
      Object index)
      throws Exception {
    Map<String, Object> map = new HashMap<>();
    map = mapper.readValue((String) section.get(JsonKey.SEARCH_QUERY), HashMap.class);
    Set<Entry<String, Object>> filterEntrySet = filterMap.entrySet();
    for (Entry<String, Object> entry : filterEntrySet) {
      if (!entry.getKey().equalsIgnoreCase(JsonKey.FILTERS)) {
        ((Map<String, Object>) map.get(JsonKey.REQUEST)).put(entry.getKey(), entry.getValue());
      }
    }
    Map<String, Object> filters =
        (Map<String, Object>) ((Map<String, Object>) map.get(JsonKey.REQUEST)).get(JsonKey.FILTERS);
    ProjectLogger.log(
        "default search query for ekstep for page data assemble api : "
            + (String) section.get(JsonKey.SEARCH_QUERY),
        LoggerEnum.INFO.name());
    applyFilters(filters, reqFilters);
    String queryRequestBody = "";

    queryRequestBody = mapper.writeValueAsString(map);
    if (StringUtils.isBlank(queryRequestBody)) {
      queryRequestBody = (String) section.get(JsonKey.SEARCH_QUERY);
    }
    ProjectLogger.log(
        "PageManagementActor:getContentData: Page assemble final search query: " + queryRequestBody,
        LoggerEnum.INFO.name());

    Future<Map<String, Object>> result =
        ContentSearchUtil.searchContent(urlQueryString, queryRequestBody, headers);

    return result.map(
        new Mapper<Map<String, Object>, Map<String, Object>>() {
          @Override
          public Map<String, Object> apply(Map<String, Object> result) {
            ProjectLogger.log(
                    "PageManagementActor:getContentData:apply: result = " + result, LoggerEnum.INFO);
            if (MapUtils.isNotEmpty(result)) {
              section.putAll(result);
              section.remove(JsonKey.PARAMS);
              Map<String, Object> tempMap = (Map<String, Object>) result.get(JsonKey.PARAMS);
              section.put(JsonKey.RES_MSG_ID, tempMap.get(JsonKey.RES_MSG_ID));
              section.put(JsonKey.API_ID, tempMap.get(JsonKey.API_ID));
              section.put(JsonKey.GROUP, group);
              section.put(JsonKey.INDEX, index);
              removeUnwantedData(section, "getPageData");
              ProjectLogger.log(
                      "PageManagementActor:getContentData:apply: section = " + section, LoggerEnum.INFO);
            }
            return section;
          }
        },
        getContext().dispatcher());
  }

  @SuppressWarnings("unchecked")
  /**
   * combine both requested page filters with default page filters.
   *
   * @param filters
   * @param reqFilters
   */
  private void applyFilters(Map<String, Object> filters, Map<String, Object> reqFilters) {
    if (null != reqFilters) {
      Set<Entry<String, Object>> entrySet = reqFilters.entrySet();
      for (Entry<String, Object> entry : entrySet) {
        String key = entry.getKey();
        if (filters.containsKey(key)) {
          Object obj = entry.getValue();
          if (obj instanceof List) {
            if (filters.get(key) instanceof List) {
              Set<Object> set = new HashSet<>((List<Object>) filters.get(key));
              set.addAll((List<Object>) obj);
              ((List<Object>) filters.get(key)).clear();
              ((List<Object>) filters.get(key)).addAll(set);
            } else if (filters.get(key) instanceof Map) {
              filters.put(key, obj);
            } else {
              if (!(((List<Object>) obj).contains(filters.get(key)))) {
                ((List<Object>) obj).add(filters.get(key));
              }
              filters.put(key, obj);
            }
          } else if (obj instanceof Map) {
            filters.put(key, obj);
          } else {
            if (filters.get(key) instanceof List) {
              if (!(((List<Object>) filters.get(key)).contains(obj))) {
                ((List<Object>) filters.get(key)).add(obj);
              }
            } else if (filters.get(key) instanceof Map) {
              filters.put(key, obj);
            } else {
              List<Object> list = new ArrayList<>();
              list.add(filters.get(key));
              list.add(obj);
              filters.put(key, list);
            }
          }
        } else {
          filters.put(key, entry.getValue());
        }
      }
    }
  }

  private Map<String, Object> getPageSetting(Map<String, Object> pageDO) {

    Map<String, Object> responseMap = new HashMap<>();
    responseMap.put(JsonKey.NAME, pageDO.get(JsonKey.NAME));
    responseMap.put(JsonKey.ID, pageDO.get(JsonKey.ID));

    if (pageDO.containsKey(JsonKey.APP_MAP) && null != pageDO.get(JsonKey.APP_MAP)) {
      responseMap.put(JsonKey.APP_SECTIONS, parsePage(pageDO, JsonKey.APP_MAP));
    }
    if (pageDO.containsKey(JsonKey.PORTAL_MAP) && null != pageDO.get(JsonKey.PORTAL_MAP)) {
      responseMap.put(JsonKey.PORTAL_SECTIONS, parsePage(pageDO, JsonKey.PORTAL_MAP));
    }
    return responseMap;
  }

  private void removeUnwantedData(Map<String, Object> map, String from) {
    map.remove(JsonKey.CREATED_DATE);
    map.remove(JsonKey.CREATED_BY);
    map.remove(JsonKey.UPDATED_DATE);
    map.remove(JsonKey.UPDATED_BY);
    if (from.equalsIgnoreCase("getPageData")) {
      map.remove(JsonKey.STATUS);
    }
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> parsePage(Map<String, Object> pageDO, String mapType) {
    List<Map<String, Object>> sections = new ArrayList<>();
    String sectionQuery = (String) pageDO.get(mapType);
    try {
      Object[] arr = mapper.readValue(sectionQuery, Object[].class);
      for (Object obj : arr) {
        Map<String, Object> sectionMap = (Map<String, Object>) obj;
        Response sectionResponse =
            cassandraOperation.getRecordById(
                pageSectionDbInfo.getKeySpace(),
                pageSectionDbInfo.getTableName(),
                (String) sectionMap.get(JsonKey.ID));

        List<Map<String, Object>> sectionResult =
            (List<Map<String, Object>>) sectionResponse.getResult().get(JsonKey.RESPONSE);
        if (null != sectionResult && !sectionResult.isEmpty()) {
          sectionResult.get(0).put(JsonKey.GROUP, sectionMap.get(JsonKey.GROUP));
          sectionResult.get(0).put(JsonKey.INDEX, sectionMap.get(JsonKey.INDEX));
          removeUnwantedData(sectionResult.get(0), "");
          sections.add(sectionResult.get(0));
        }
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return sections;
  }

  private void validateOrg(String orgId) {
    Response result =
        cassandraOperation.getRecordById(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgId);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(list)) {
      throw new ProjectCommonException(
          ResponseCode.invalidOrgId.getErrorCode(),
          ResponseCode.invalidOrgId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }
}
