package org.sunbird.learner.actors;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;

/** @author kirti. Junit test cases */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class, DataCacheHandler.class, CassandraOperation.class})
@PowerMockIgnore("javax.management.*")
public class UtilTest {

  private static CassandraOperationImpl mockCassandraOperation;

  @Before
  public void setUp() {
    PowerMockito.mockStatic(ServiceFactory.class);
    PowerMockito.mockStatic(DataCacheHandler.class);
    mockCassandraOperation = Mockito.mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(mockCassandraOperation);
  }

  @Test
  public void testCheckPhoneUniqnessFailureCreate() {

    Map<String, String> map = new HashMap<>();
    map.put(JsonKey.PHONE, JsonKey.UNIQUE);

    when(DataCacheHandler.getConfigSettings()).thenReturn(map);
    Response response = createCassandraResponse();

    Map<String, Object> userMap = createUserMap();
    // during create we don't need Id
    userMap.remove(JsonKey.ID);
    when(mockCassandraOperation.getRecordsByIndexedProperty(
            "sunbird", "user", "phone", "1234567890"))
        .thenReturn(response);
    Exception e = null;
    try {
      Util.checkPhoneUniqueness(userMap, JsonKey.CREATE);
    } catch (Exception ex) {
      e = ex;
    }
    Assert.assertTrue(
        ((ProjectCommonException) e)
            .getMessage()
            .equals(ResponseCode.PhoneNumberInUse.getErrorMessage()));
  }

  @Test
  public void testCheckPhoneUniqnessFailureUpdate() {

    Map<String, String> map = new HashMap<>();
    map.put(JsonKey.PHONE, JsonKey.UNIQUE);

    when(DataCacheHandler.getConfigSettings()).thenReturn(map);
    Response response = createCassandraResponse();
    when(mockCassandraOperation.getRecordsByIndexedProperty(
            "sunbird", "user", "phone", "1234567890"))
        .thenReturn(response);
    Exception e = null;
    try {
      Util.checkPhoneUniqueness(createUserMap(), JsonKey.UPDATE);
    } catch (Exception ex) {
      e = ex;
    }
    Assert.assertTrue(
        ((ProjectCommonException) e)
            .getMessage()
            .equals(ResponseCode.PhoneNumberInUse.getErrorMessage()));
  }

  @Test
  public void testCheckPhoneUniqnessSuccessCreate() {

    Map<String, String> map = new HashMap<>();
    map.put(JsonKey.PHONE, JsonKey.UNIQUE);

    when(DataCacheHandler.getConfigSettings()).thenReturn(map);
    Response response = createCassandraNullResponse();
    when(mockCassandraOperation.getRecordsByIndexedProperty(
            "sunbird", "user", "phone", "1234567890"))
        .thenReturn(response)
        .thenReturn(response);
    Util.checkPhoneUniqueness(createUserMap(), JsonKey.CREATE);
    String testResult = "success";
    Assert.assertTrue(JsonKey.SUCCESS.equalsIgnoreCase((testResult)));
  }

  @Test
  public void testCheckPhoneUniqnessSuccessUpdate() {

    Map<String, String> map = new HashMap<>();
    map.put(JsonKey.PHONE, JsonKey.UNIQUE);

    when(DataCacheHandler.getConfigSettings()).thenReturn(map);
    Response response = createCassandraNullResponse();
    mockCassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(mockCassandraOperation);
    when(mockCassandraOperation.getRecordsByIndexedProperty(
            "sunbird", "user", "phone", "1234567890"))
        .thenReturn(response);
    Util.checkPhoneUniqueness(createUserMap(), JsonKey.UPDATE);
    String testResult = "success";
    Assert.assertTrue(JsonKey.SUCCESS.equalsIgnoreCase((testResult)));
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
    Map<String, Object> bulkUploadProcessMap = createUserMap();
    bulkUploadProcessMap.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());

    list.add(bulkUploadProcessMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private Map<String, Object> createUserMap() {
    Map<String, Object> userMap = new HashMap<>();
    userMap.put(JsonKey.ID, "123xyz");
    userMap.put(JsonKey.LAST_NAME, "xyz");
    userMap.put(JsonKey.FIRST_NAME, "abc");
    userMap.put(JsonKey.ORGANISATION_ID, "12345");
    userMap.put(JsonKey.PHONE, "1234567890");
    userMap.put(JsonKey.EMAIL, "abc@gmail.com");
    userMap.put(JsonKey.ROOT_ORG_ID, "123");
    return userMap;
  }
}
