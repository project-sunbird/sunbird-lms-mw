package org.sunbird.learner;

import static org.powermock.api.mockito.PowerMockito.when;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.CassandraUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.helper.CassandraConnectionManager;
import org.sunbird.helper.CassandraConnectionManagerImpl;
import org.sunbird.helper.CassandraConnectionMngrFactory;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;

/** @author kirti. Junit test cases */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  DataCacheHandler.class,
  CassandraConnectionManagerImpl.class,
  Select.Selection.class,
  Select.Builder.class,
  CassandraUtil.class,
  QueryBuilder.class,
  Select.Where.class,
  Session.class,
  CassandraConnectionMngrFactory.class
})
@PowerMockIgnore("javax.management.*")
public class UtilTest {

  private static CassandraConnectionManager cassandraConnectionManager;
  private static Session session = PowerMockito.mock(Session.class);
  private static Select.Selection selectSelection;
  private static Select.Builder selectBuilder;
  private static PropertiesCache cach = PropertiesCache.getInstance();
  private static String cassandraKeySpace = cach.getProperty("keyspace");
  private static Select selectQuery;
  private static ResultSet result;
  private static Select.Where where;

  @BeforeClass
  public static void Init() {

    cassandraConnectionManager = PowerMockito.mock(CassandraConnectionManagerImpl.class);
  }

  @Before
  public void setUp() {

    Map<String, String> map = new HashMap<>();
    map.put(JsonKey.PHONE_UNIQUE, "TRUE");
    PowerMockito.mockStatic(DataCacheHandler.class);
    when(DataCacheHandler.getConfigSettings()).thenReturn(map);

    PowerMockito.mockStatic(QueryBuilder.class);
    selectQuery = PowerMockito.mock(Select.class);
    selectSelection = PowerMockito.mock(Select.Selection.class);
    when(QueryBuilder.select()).thenReturn(selectSelection);
    result = PowerMockito.mock(ResultSet.class);

    selectBuilder = PowerMockito.mock(Select.Builder.class);
    when(selectSelection.all()).thenReturn(selectBuilder);
    when(selectBuilder.from(Mockito.anyString(), Mockito.anyString())).thenReturn(selectQuery);
    QueryBuilder.select().all().from(cassandraKeySpace, "user");

    PowerMockito.mockStatic(CassandraConnectionMngrFactory.class);
    when(CassandraConnectionMngrFactory.getObject(Mockito.anyString()))
        .thenReturn(cassandraConnectionManager);

    when(cassandraConnectionManager.getSession("sunbird")).thenReturn(session);
    PowerMockito.mockStatic(CassandraUtil.class);

    where = PowerMockito.mock(Select.Where.class);
    when(selectQuery.where()).thenReturn(where);
    where.and(QueryBuilder.eq("phone", "1234567890"));

    when(selectQuery.allowFiltering()).thenReturn(selectQuery);
    when(session.execute(selectQuery)).thenReturn(result);
  }

  @Test
  public void testCheckPhoneUniqnessFailureCreate() {

    Map<String, Object> userMap = new HashMap<>();

    userMap = new HashMap<>();
    userMap.put(JsonKey.ID, "123xyz");
    userMap.put(JsonKey.LAST_NAME, "xyz");
    userMap.put(JsonKey.FIRST_NAME, "abc");
    userMap.put(JsonKey.ORGANISATION_ID, "12345");
    userMap.put(JsonKey.PHONE, "1234567890");
    userMap.put(JsonKey.EMAIL, "abc@gmail.com");
    userMap.put(JsonKey.ROOT_ORG_ID, "123");

    String opType = "CREATE";
    Response response = createCassandraResponse();
    when(CassandraUtil.createResponse(result)).thenReturn(response);

    Exception exp = null;
    try {
      Util.checkPhoneUniqueness(userMap, opType);
    } catch (Exception ex) {
      exp = ex;
    }
    Assert.assertTrue(
        "Phone already in use. Please provide different phone number.".equals(exp.getMessage()));
  }

  @Test
  public void testCheckPhoneUniqnessFailureUpdate() {

    Map<String, Object> userMap = new HashMap<>();

    userMap = new HashMap<>();
    userMap.put(JsonKey.ID, "123xyz");
    userMap.put(JsonKey.LAST_NAME, "xyz");
    userMap.put(JsonKey.FIRST_NAME, "abc");
    userMap.put(JsonKey.ORGANISATION_ID, "12345");
    userMap.put(JsonKey.PHONE, "1234567890");
    userMap.put(JsonKey.EMAIL, "abc@gmail.com");
    userMap.put(JsonKey.ROOT_ORG_ID, "123");

    String opType = "UPDATE";
    Response response = createCassandraResponse();
    when(CassandraUtil.createResponse(result)).thenReturn(response);

    Exception exp = null;
    try {
      Util.checkPhoneUniqueness(userMap, opType);
    } catch (Exception ex) {
      exp = ex;
    }
    Assert.assertTrue(
        "Phone already in use. Please provide different phone number.".equals(exp.getMessage()));
  }

  @Test
  public void testCheckPhoneUniqnessSuccessCreate() {

    Map<String, Object> userMap = new HashMap<>();

    userMap = new HashMap<>();
    userMap.put(JsonKey.ID, "123xyz");
    userMap.put(JsonKey.LAST_NAME, "xyz");
    userMap.put(JsonKey.FIRST_NAME, "abc");
    userMap.put(JsonKey.ORGANISATION_ID, "12345");
    userMap.put(JsonKey.PHONE, "1234567890");
    userMap.put(JsonKey.EMAIL, "abc@gmail.com");
    userMap.put(JsonKey.ROOT_ORG_ID, "123");

    String opType = "CREATE";
    Response response = createCassandraNullResponse();
    when(CassandraUtil.createResponse(result)).thenReturn(response);

    Exception exp = null;
    try {
      Util.checkPhoneUniqueness(userMap, opType);
    } catch (Exception ex) {
      exp = ex;
    }
    Assert.assertTrue(exp == null);
  }

  @Test
  public void testCheckPhoneUniqnessSuccessUpdate() {

    Map<String, Object> userMap = new HashMap<>();

    userMap = new HashMap<>();
    userMap.put(JsonKey.ID, "qwerty123");
    userMap.put(JsonKey.LAST_NAME, "xyz");
    userMap.put(JsonKey.FIRST_NAME, "abc");
    userMap.put(JsonKey.ORGANISATION_ID, "12345");
    userMap.put(JsonKey.PHONE, "1234567891");
    userMap.put(JsonKey.EMAIL, "abc@gmail.com");
    userMap.put(JsonKey.ROOT_ORG_ID, "123");

    String opType = "UPDATE";
    Response response = createCassandraResponse();
    when(CassandraUtil.createResponse(result)).thenReturn(response);

    Exception exp = null;
    try {
      Util.checkPhoneUniqueness(userMap, opType);
    } catch (Exception ex) {
      exp = ex;
    }
    Assert.assertTrue(exp == null);
  }

  private Response createCassandraNullResponse() {

    Response response = new Response();
    List<Map<String, Object>> responseList = new ArrayList();
    response.put(JsonKey.RESPONSE, responseList);
    return response;
  }

  private Response createCassandraResponse() {

    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> bulkUploadProcessMap = new HashMap<>();
    bulkUploadProcessMap.put(JsonKey.ID, "qwerty123");
    bulkUploadProcessMap.put(JsonKey.LAST_NAME, "singh");
    bulkUploadProcessMap.put(JsonKey.CHANNEL, "abc");
    bulkUploadProcessMap.put(JsonKey.PASSWORD, "qwerty");
    bulkUploadProcessMap.put(JsonKey.EMAIL, "abc123@gmail.com");
    bulkUploadProcessMap.put(JsonKey.FIRST_NAME, "rajiv");
    bulkUploadProcessMap.put(JsonKey.PHONE, "1234567890");
    bulkUploadProcessMap.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());

    list.add(bulkUploadProcessMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }
}
