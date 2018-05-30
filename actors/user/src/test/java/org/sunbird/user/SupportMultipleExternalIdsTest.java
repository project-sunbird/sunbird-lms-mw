package org.sunbird.user;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;

/** @author Amit Kumar */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ElasticSearchUtil.class,
  CassandraOperationImpl.class,
  Util.class,
  ServiceFactory.class,
})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class SupportMultipleExternalIdsTest {
  private static List<Map<String, String>> externalIds = new ArrayList<>();
  private static CassandraOperation cassandraOperation = null;
  private static User user = null;
  private static Map<String, String> externalIdResMap3 = null;

  @BeforeClass
  public static void setUp() throws Exception {
    Map<String, String> externalIdReqMap1 = new HashMap<>();
    externalIdReqMap1.put(JsonKey.ID, "123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf");
    externalIdReqMap1.put(JsonKey.PROVIDER, "AADHAR");

    Map<String, String> externalIdReqMap2 = new HashMap<>();
    externalIdReqMap2.put(JsonKey.ID, "123209-453445934-23128u3423-dsafsa32c43-few43-qwe34sf34");
    externalIdReqMap2.put(JsonKey.PROVIDER, "PAN");

    externalIds.add(externalIdReqMap1);
    externalIds.add(externalIdReqMap2);

    user = new User();
    user.setExternalIds(externalIds);

    Map<String, String> externalIdResMap1 = new HashMap<>();
    externalIdResMap1.put(JsonKey.ID, "123209");
    externalIdResMap1.put(JsonKey.PROVIDER, "AADHAR");
    externalIdResMap1.put(JsonKey.USER_ID, "12365824-79812023-asd7899-121xasdd5");
    externalIdResMap1.put(
        JsonKey.EXTERNAL_ID, "123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf");

    Map<String, String> externalIdResMap2 = new HashMap<>();
    externalIdResMap2.put(JsonKey.ID, "123210");
    externalIdResMap2.put(JsonKey.PROVIDER, "pan");
    externalIdResMap1.put(JsonKey.USER_ID, "12365824-79812023-asd7899-121xasdd5");
    externalIdResMap2.put(
        JsonKey.EXTERNAL_ID, "123209-453445934-23128u3423-dsafsa32c43-few43-qwe34sf34");

    externalIds.add(externalIdResMap1);
    externalIds.add(externalIdResMap2);

    externalIdResMap3 = new HashMap<>();
    externalIdResMap3.put(JsonKey.ID, "123209");
    externalIdResMap3.put(JsonKey.PROVIDER, "AADHAR");
    externalIdResMap3.put(JsonKey.USER_ID, "12365824-79812023-asd7899-121xasdd512");
    externalIdResMap3.put(
        JsonKey.EXTERNAL_ID, "123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf456");

    Map<String, String> externalIdResMap4 = new HashMap<>();
    externalIdResMap4.put(JsonKey.ID, "123210");
    externalIdResMap4.put(JsonKey.PROVIDER, "PAN");
    externalIdResMap4.put(JsonKey.USER_ID, "12365824-79812023-asd7899-121xasdd512");
    externalIdResMap4.put(
        JsonKey.EXTERNAL_ID, "123209-453445934-23128u3423-dsafsa32c43-few43-qwe34sf34456");

    externalIds.add(externalIdResMap3);
    externalIds.add(externalIdResMap4);

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = PowerMockito.mock(CassandraOperationImpl.class);
    PowerMockito.when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    Response response1 = new Response();
    List<Map<String, String>> resMapList = new ArrayList<>();
    resMapList.add(externalIdResMap1);
    response1.getResult().put(JsonKey.RESPONSE, resMapList);
    PowerMockito.when(
            cassandraOperation.getRecordsByProperties(
                Mockito.any(), Mockito.any(), Mockito.anyMap()))
        .thenReturn(response1);
  }

  @Test
  public void testCheckExternalIdAndProviderUniquenessForCreate() {
    try {
      Util.checkExternalIdAndProviderUniqueness(user, JsonKey.CREATE);
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
      assertTrue(
          ex.getMessage()
              .equalsIgnoreCase(
                  "User already exists for given externalId : 123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf and provider : AADHAR."));
    }
  }

  // will try to delete other user extIds
  @Test
  public void testCheckExternalIdAndProviderUniquenessForUpdate() {
    Response response = new Response();
    List<Map<String, String>> resMapList = new ArrayList<>();
    resMapList.add(externalIdResMap3);
    response.getResult().put(JsonKey.RESPONSE, resMapList);
    PowerMockito.when(
            cassandraOperation.getRecordsByProperties(
                Mockito.any(), Mockito.any(), Mockito.anyMap()))
        .thenReturn(response);
    try {
      user.setUserId("456456");
      user.getExternalIds().get(0).put(JsonKey.OPERATION, JsonKey.DELETE);
      Util.checkExternalIdAndProviderUniqueness(user, JsonKey.UPDATE);
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
      assertTrue(
          ex.getMessage()
              .equalsIgnoreCase(
                  "External ID (id: 123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf, provider: AADHAR) not found for given user."));
    }
  }

  // will try to update other user extIds
  @Test
  public void testCheckExternalIdAndProviderUniquenessForUpdate2() {
    Response response = new Response();
    List<Map<String, String>> resMapList = new ArrayList<>();
    resMapList.add(externalIdResMap3);
    response.getResult().put(JsonKey.RESPONSE, resMapList);
    PowerMockito.when(
            cassandraOperation.getRecordsByProperties(
                Mockito.any(), Mockito.any(), Mockito.anyMap()))
        .thenReturn(response);
    try {
      user.setUserId("456456");
      user.getExternalIds().get(0).put(JsonKey.OPERATION, JsonKey.UPDATE);
      Util.checkExternalIdAndProviderUniqueness(user, JsonKey.UPDATE);
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
      assertTrue(
          ex.getMessage()
              .equalsIgnoreCase(
                  "External ID (id: 123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf, provider: AADHAR) not found for given user."));
    }
  }

  // will try to delete non existing extIds
  @Test
  public void testCheckExternalIdAndProviderUniquenessForUpdate3() {
    Response response = new Response();
    List<Map<String, String>> resMapList = new ArrayList<>();
    response.getResult().put(JsonKey.RESPONSE, resMapList);
    PowerMockito.when(
            cassandraOperation.getRecordsByProperties(
                Mockito.any(), Mockito.any(), Mockito.anyMap()))
        .thenReturn(response);
    try {
      user.setUserId("456456");
      user.getExternalIds().get(0).put(JsonKey.OPERATION, JsonKey.UPDATE);
      Util.checkExternalIdAndProviderUniqueness(user, JsonKey.UPDATE);
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
      assertTrue(
          ex.getMessage()
              .equalsIgnoreCase(
                  "External ID (id: 123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf, provider: AADHAR) not found for given user."));
    }
  }
}
