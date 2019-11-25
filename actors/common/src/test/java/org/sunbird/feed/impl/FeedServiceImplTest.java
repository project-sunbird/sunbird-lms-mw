package org.sunbird.feed.impl;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.Constants;
import org.sunbird.common.ElasticSearchHelper;
import org.sunbird.common.ElasticSearchRestHighImpl;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.inf.ElasticSearchService;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.feed.IFeedService;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.models.user.Feed;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  ElasticSearchRestHighImpl.class,
  ElasticSearchHelper.class,
  EsClientFactory.class,
  CassandraOperationImpl.class,
  ElasticSearchService.class,
  FeedServiceImpl.class,
})
@SuppressStaticInitializationFor("org.sunbird.common.ElasticSearchUtil")
@PowerMockIgnore({"javax.management.*"})
public class FeedServiceImplTest {
  private ElasticSearchService esUtil;
  private static CassandraOperation cassandraOperation = null;
  private static IFeedService feedService = FeedFactory.getInstance();

  @Before
  public void setUp() throws Exception {
    PowerMockito.mockStatic(ServiceFactory.class);
    PowerMockito.mockStatic(EsClientFactory.class);
    PowerMockito.mockStatic(ElasticSearchHelper.class);
    PowerMockito.mockStatic(FeedServiceImpl.class);
    esUtil = mock(ElasticSearchService.class);
    esUtil = mock(ElasticSearchRestHighImpl.class);
    when(EsClientFactory.getInstance(Mockito.anyString())).thenReturn(esUtil);
    cassandraOperation = mock(CassandraOperationImpl.class);
    PowerMockito.when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(FeedServiceImpl.getCassandraInstance()).thenReturn(cassandraOperation);
    when(FeedServiceImpl.getESInstance()).thenReturn(esUtil);
  }

  private static void initCassandraForSuccess() {
    Response response = new Response();
    Map<String, Object> responseMap = new HashMap<>();
    responseMap.put(Constants.RESPONSE, Arrays.asList(getFeedMap()));
    response.getResult().putAll(responseMap);
    PowerMockito.when(
            cassandraOperation.getRecordsByProperties(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(response);

    Response upsertResponse = new Response();
    Map<String, Object> responseMap2 = new HashMap<>();
    responseMap2.put(Constants.RESPONSE, Constants.SUCCESS);
    upsertResponse.getResult().putAll(responseMap2);
    PowerMockito.when(cassandraOperation.upsertRecord(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(upsertResponse);
    PowerMockito.when(
            cassandraOperation.deleteRecord(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(upsertResponse);
  }

  @Test
  public void testInsert() {
    initCassandraForSuccess();
    Response res = feedService.insert(getFeed(false));
    Assert.assertTrue(
        ((String) res.getResult().get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS));
  }

  @Test
  public void testUpdate() {
    initCassandraForSuccess();
    Response res = feedService.update(getFeed(true));
    Assert.assertTrue(
        ((String) res.getResult().get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS));
  }

  @Test
  public void testDelete() {
    initCassandraForSuccess();
    try {
      feedService.delete("123-456-789");
    } catch (Exception ex) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testGetRecordsByProperties() {
    initCassandraForSuccess();
    Map<String, Object> props = new HashMap<>();
    props.put(JsonKey.USER_ID, "123-456-789");
    List<Feed> res = feedService.getRecordsByProperties(props);
    Assert.assertTrue(res != null);
  }

  private static Map<String, Object> getFeedMap() {
    Map<String, Object> fMap = new HashMap<>();
    fMap.put(JsonKey.ID, "123-456-7890");
    fMap.put(JsonKey.USER_ID, "123-456-789");
    fMap.put(JsonKey.CATEGORY, "category");
    return fMap;
  }

  private Feed getFeed(boolean needId) {
    Feed feed = new Feed();
    feed.setUserId("123-456-7890");
    feed.setCategory("category");
    if (needId) {
      feed.setId("123-456-789");
    }
    Map<String, Object> map = new HashMap<>();
    List<String> channelList = new ArrayList<>();
    channelList.add("SI");
    map.put(JsonKey.PROSPECT_CHANNELS, channelList);
    feed.setData(map);
    return feed;
  }
}
