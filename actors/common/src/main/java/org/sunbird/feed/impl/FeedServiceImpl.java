package org.sunbird.feed.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchHelper;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.inf.ElasticSearchService;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.dto.SearchDTO;
import org.sunbird.feed.IFeedService;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.Feed;
import scala.concurrent.Future;

public class FeedServiceImpl implements IFeedService {
  private ElasticSearchService esService = EsClientFactory.getInstance(JsonKey.REST);
  private Util.DbInfo usrFeedDbInfo = Util.dbInfoMap.get(JsonKey.USER_FEED_DB);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static volatile IFeedService instance;

  private FeedServiceImpl() {}

  public static IFeedService getInstance() {
    if (instance == null) {
      synchronized (FeedServiceImpl.class) {
        if (instance == null) instance = new FeedServiceImpl();
      }
    }
    return instance;
  }

  @Override
  public Response insert(Feed feed) {
    ProjectLogger.log("FeedServiceImpl:insert method called : ", LoggerEnum.INFO.name());
    Map<String, Object> feedData = feed.getData();
    Map<String, Object> dbMap = mapper.convertValue(feed, Map.class);
    String feedId = ProjectUtil.generateUniqueId();
    dbMap.put(JsonKey.ID, feedId);
    dbMap.put(JsonKey.CREATED_ON, new Timestamp(Calendar.getInstance().getTimeInMillis()));
    try {
      dbMap.put(JsonKey.FEED_DATA, mapper.writeValueAsString(feed.getData()));
    } catch (Exception ex) {
      ProjectLogger.log("FeedServiceImpl:insert Exception occurred while mapping.", ex);
    }
    Response response = saveFeed(dbMap);
    // save data to ES
    dbMap.put(JsonKey.FEED_DATA, feedData);
    dbMap.put(JsonKey.CREATED_ON, Calendar.getInstance().getTimeInMillis());
    esService.save(ProjectUtil.EsType.userfeed.getTypeName(), feedId, dbMap);
    return response;
  }

  @Override
  public Response update(Feed feed) {
    ProjectLogger.log("FeedServiceImpl:update method called : ", LoggerEnum.INFO.name());
    Map<String, Object> feedData = feed.getData();
    Map<String, Object> dbMap = mapper.convertValue(feed, Map.class);
    try {
      dbMap.put(JsonKey.FEED_DATA, mapper.writeValueAsString(feed.getData()));
    } catch (Exception ex) {
      ProjectLogger.log("FeedServiceImpl:update Exception occurred while mapping.", ex);
    }
    dbMap.put(JsonKey.UPDATED_ON, new Timestamp(Calendar.getInstance().getTimeInMillis()));
    Response response = saveFeed(dbMap);
    // update data to ES
    dbMap.put(JsonKey.FEED_DATA, feedData);
    dbMap.put(JsonKey.UPDATED_ON, Calendar.getInstance().getTimeInMillis());
    esService.update(ProjectUtil.EsType.userfeed.getTypeName(), feed.getId(), dbMap);
    return response;
  }

  @Override
  public List<Feed> getRecordsByProperties(Map<String, Object> properties) {
    ProjectLogger.log(
        "FeedServiceImpl:getRecordsByProperties method called : ", LoggerEnum.INFO.name());
    Response dbResponse =
        cassandraOperation.getRecordsByProperties(
            usrFeedDbInfo.getKeySpace(), usrFeedDbInfo.getTableName(), properties);
    List<Map<String, Object>> responseList = null;
    List<Feed> feedList = new ArrayList<>();
    Response response = null;
    if (null != dbResponse && null != dbResponse.getResult()) {
      responseList = (List<Map<String, Object>>) dbResponse.getResult().get(JsonKey.RESPONSE);
      if (CollectionUtils.isNotEmpty(responseList)) {
        responseList.forEach(
            s -> {
              String data = (String) s.get(JsonKey.FEED_DATA);
              s.put(JsonKey.FEED_DATA, mapper.convertValue(data, Map.class));
              feedList.add(mapper.convertValue(s, Feed.class));
            });
      }
    }
    return feedList;
  }

  @Override
  public Response search(SearchDTO searchDTO) {
    ProjectLogger.log("FeedServiceImpl:search method called : ", LoggerEnum.INFO.name());
    Future<Map<String, Object>> resultF =
        esService.search(searchDTO, ProjectUtil.EsType.userfeed.getTypeName());
    Map<String, Object> result =
        (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(resultF);
    Response response = new Response();
    response.put(JsonKey.RESPONSE, result);
    return response;
  }

  @Override
  public void delete(String id) {
    ProjectLogger.log(
        "FeedServiceImpl:delete method called for feedId : " + id, LoggerEnum.INFO.name());
    cassandraOperation.deleteRecord(usrFeedDbInfo.getKeySpace(), usrFeedDbInfo.getTableName(), id);
    esService.delete(ProjectUtil.EsType.userfeed.getTypeName(), id);
  }

  private Response saveFeed(Map<String, Object> feed) {
    return cassandraOperation.upsertRecord(
        usrFeedDbInfo.getKeySpace(), usrFeedDbInfo.getTableName(), feed);
  }
}
