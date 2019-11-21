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

  @Override
  public Response save(Feed feed) {
    Response response = null;
    if (StringUtils.isNotBlank(feed.getId())) {
      ProjectLogger.log(
          "FeedServiceImpl:save Calling updateUserFeed for feed id : " + feed.getId(),
          LoggerEnum.INFO.name());
      response = updateUserFeed(feed);
    } else {
      ProjectLogger.log("FeedServiceImpl:save Calling insertUserFeed", LoggerEnum.INFO.name());
      response = insertUserFeed(feed);
    }
    return response;
  }

  private Response insertUserFeed(Feed feed) {
    Map<String, Object> userFeed = mapper.convertValue(feed, Map.class);
    userFeed.put(JsonKey.ID, ProjectUtil.generateUniqueId());
    userFeed.put(JsonKey.CREATED_ON, new Timestamp(Calendar.getInstance().getTimeInMillis()));
    Response response = saveFeed(userFeed);
    esService.save(ProjectUtil.EsType.userfeed.getTypeName(), feed.getId(), userFeed);
    return response;
  }

  private Response updateUserFeed(Feed feed) {
    ProjectLogger.log(
        "FeedServiceImpl:updateUserFeed fetching getRecordById for feed id " + feed.getId(),
        LoggerEnum.INFO.name());
    Response response =
        cassandraOperation.getRecordById(
            usrFeedDbInfo.getKeySpace(), usrFeedDbInfo.getTableName(), feed.getId());
    Map<String, Object> userFeed = new HashMap<>();
    if (null != response && null != response.getResult()) {
      List<Map<String, Object>> userFeedList =
          (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
      if (CollectionUtils.isNotEmpty(userFeedList)) {
        userFeed = userFeedList.get(0);
      }
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
    List<String> userChannel = (List<String>) feedDataMap.get(JsonKey.CHANNEL);
    userChannel.addAll((List<String>) feed.getFeedData().get(JsonKey.CHANNEL));
    userFeed.put(JsonKey.UPDATED_ON, new Timestamp(Calendar.getInstance().getTimeInMillis()));
    userFeed.put(JsonKey.FEED_DATA, mapper.convertValue(feedDataMap, String.class));
    Response userFeedResponse = saveFeed(userFeed);
    esService.save(ProjectUtil.EsType.userfeed.getTypeName(), feed.getId(), userFeed);
    return userFeedResponse;
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
