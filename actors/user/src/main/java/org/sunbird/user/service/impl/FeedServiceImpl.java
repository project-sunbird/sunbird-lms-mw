package org.sunbird.user.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.util.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.Feed;
import org.sunbird.user.service.IFeedService;
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
  public Response save(Feed feed) {
    ProjectLogger.log("FeedServiceImpl:save Calling save UserFeed : ", LoggerEnum.INFO.name());
    esService = EsClientFactory.getInstance(JsonKey.REST);
    usrFeedDbInfo = Util.dbInfoMap.get(JsonKey.USER_FEED_DB);
    cassandraOperation = ServiceFactory.getInstance();
    Response response = null;
    if (StringUtils.isNotBlank(feed.getId())) {
      response = updateUserFeed(feed);
    } else {
      response = saveUserFeed(feed);
    }
    return response;
  }

  private Response saveUserFeed(Feed feed) {
    Map<String, Object> userFeed = mapper.convertValue(feed, Map.class);
    userFeed.put(JsonKey.ID, ProjectUtil.generateUniqueId());
    userFeed.put(JsonKey.CREATED_ON, new Timestamp(Calendar.getInstance().getTimeInMillis()));
    // To store feed data as String in cassandra
    userFeed.put(JsonKey.FEED_DATA, mapper.convertValue(feed.getData(), String.class));
    Response response = saveFeed(userFeed);
    // store feed data as Map in ES
    userFeed.put(JsonKey.FEED_DATA, feed.getData());
    esService.save(ProjectUtil.EsType.userfeed.getTypeName(), feed.getId(), userFeed);
    return response;
  }

  private Response updateUserFeed(Feed feed) {
    ProjectLogger.log(
        "FeedServiceImpl:updateUserFeed for feed id " + feed.getId(), LoggerEnum.INFO.name());
    Map<String, Object> userFeed = new HashMap<>();
    List<Map<String, Object>> userFeedList = getFeedById(feed.getId());
    if (CollectionUtils.isNotEmpty(userFeedList)) {
      userFeed = userFeedList.get(0);
    }
    String feedData = (String) userFeed.get(JsonKey.FEED_DATA);
    Map<String, Object> feedDataMap = new HashMap<>();
    try {
      feedDataMap = mapper.readValue(feedData, new TypeReference<Map<String, Object>>() {});
    } catch (Exception ex) {
      ProjectLogger.log(
          "FeedServiceImpl:save: Exception occurred while converting feed data from db to List",
          ex);
    }
    List<String> userProspectsChannel = (List<String>) feedDataMap.get(JsonKey.PROSPECTS_CHANNELS);
    userProspectsChannel.addAll((List<String>) feed.getData().get(JsonKey.PROSPECTS_CHANNELS));
    userFeed.put(JsonKey.UPDATED_ON, new Timestamp(Calendar.getInstance().getTimeInMillis()));
    // Store feed data as String in cassandra
    userFeed.put(JsonKey.FEED_DATA, mapper.convertValue(feedDataMap, String.class));
    Response userFeedResponse = saveFeed(userFeed);
    // Store feed data as Map in ES
    userFeed.put(JsonKey.FEED_DATA, mapper.convertValue(feedDataMap, Map.class));
    esService.save(ProjectUtil.EsType.userfeed.getTypeName(), feed.getId(), userFeed);
    return userFeedResponse;
  }

  private List<Map<String, Object>> getFeedById(String feedId) {
    ProjectLogger.log(
        "FeedServiceImpl:updateUserFeed fetching getRecordById for feed id " + feedId,
        LoggerEnum.INFO.name());
    Response response =
        cassandraOperation.getRecordById(
            usrFeedDbInfo.getKeySpace(), usrFeedDbInfo.getTableName(), feedId);
    if (null != response && null != response.getResult()) {
      return (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    }
    return Collections.EMPTY_LIST;
  }

  @Override
  public Response search(SearchDTO searchDTO) {
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
    cassandraOperation.deleteRecord(usrFeedDbInfo.getKeySpace(), usrFeedDbInfo.getTableName(), id);
  }

  public Response saveFeed(Map<String, Object> feed) {
    return cassandraOperation.upsertRecord(
        usrFeedDbInfo.getKeySpace(), usrFeedDbInfo.getTableName(), feed);
  }
}
