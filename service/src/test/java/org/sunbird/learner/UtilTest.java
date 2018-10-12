package org.sunbird.learner;

import static org.powermock.api.mockito.PowerMockito.when;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
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
import org.sunbird.common.models.util.JsonKey;
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
  private static Map<String, Object> userMap;

  @BeforeClass
  public static void Init() {

    userMap = new HashMap<>();
    userMap = new HashMap<>();
    userMap.put(JsonKey.ID, "qwerty123");
    userMap.put(JsonKey.LAST_NAME, "xyz");
    userMap.put(JsonKey.FIRST_NAME, "abc");
    userMap.put(JsonKey.ORGANISATION_ID, "12345");
    userMap.put(JsonKey.PHONE, "1234567891");
    userMap.put(JsonKey.EMAIL, "abc@gmail.com");
    userMap.put(JsonKey.ROOT_ORG_ID, "123");
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
    where = PowerMockito.mock(Select.Where.class);
    when(selectQuery.where()).thenReturn(where);
    where.and(QueryBuilder.eq("phone", "1234567890"));
    when(selectQuery.allowFiltering()).thenReturn(selectQuery);
    when(session.execute(selectQuery)).thenReturn(result);
    ColumnDefinitions cd = PowerMockito.mock(ColumnDefinitions.class);
    when(result.getColumnDefinitions()).thenReturn(cd);
    String str = "qkeytypoid(king";
    when(cd.toString()).thenReturn(str);
    when(str.substring(8, result.getColumnDefinitions().toString().length() - 1)).thenReturn(str);
  }

  @Test
  public void testCheckPhoneUniqnessFailureCreate() {

    List<Row> rows = new ArrayList<>();
    Row row = Mockito.mock(Row.class);
    rows.add(row);
    when(result.all()).thenReturn(rows);

    String opType = "CREATE";
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

    String opType = "UPDATE";
    List<Row> rows = new ArrayList<>();
    Row row = Mockito.mock(Row.class);
    rows.add(row);
    when(result.all()).thenReturn(rows);
    when(row.getObject("id")).thenReturn("123xyz");

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

    String opType = "CREATE";
    List<Row> rows = new ArrayList<>();
    Row row = Mockito.mock(Row.class);
    when(result.all()).thenReturn(rows);
    when(row.getObject("id")).thenReturn("123xyz");

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

    String opType = "UPDATE";
    List<Row> rows = new ArrayList<>();
    Row row = Mockito.mock(Row.class);
    rows.add(row);
    when(result.all()).thenReturn(rows);
    when(row.getObject("id")).thenReturn("qwerty123");

    Exception exp = null;
    try {
      Util.checkPhoneUniqueness(userMap, opType);
    } catch (Exception ex) {
      exp = ex;
    }
    Assert.assertTrue(exp == null);
  }
}
