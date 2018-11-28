package org.sunbird.user;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
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

  @BeforeClass
  public static void setUp() throws Exception {
    Map<String, String> externalIdReqMap1 = new HashMap<>();
    externalIdReqMap1.put(JsonKey.ID, "123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf");
    externalIdReqMap1.put(JsonKey.PROVIDER, "AP");
    externalIdReqMap1.put(JsonKey.ID_TYPE, "AAADHAR");

    Map<String, String> externalIdReqMap2 = new HashMap<>();
    externalIdReqMap2.put(JsonKey.ID, "123209-453445934-23128u3423-dsafsa32c43-few43-qwe34sf34");
    externalIdReqMap2.put(JsonKey.PROVIDER, "AP");
    externalIdReqMap2.put(JsonKey.ID_TYPE, "PAN");

    externalIds.add(externalIdReqMap1);
    externalIds.add(externalIdReqMap2);

    user = new User();
    user.setExternalIds(externalIds);

    Map<String, String> externalIdResMap = new HashMap<>();
    externalIdResMap.put(JsonKey.PROVIDER, "AP");
    externalIdResMap.put(JsonKey.ID_TYPE, "AAADHAR");
    externalIdResMap.put(JsonKey.USER_ID, "12365824-79812023-asd7899-121xasdd5");
    externalIdResMap.put(
        JsonKey.EXTERNAL_ID, "123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf");

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = PowerMockito.mock(CassandraOperationImpl.class);
    PowerMockito.when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    Response response1 = new Response();
    List<Map<String, String>> resMapList = new ArrayList<>();
    resMapList.add(externalIdResMap);
    response1.getResult().put(JsonKey.RESPONSE, resMapList);
    PowerMockito.when(
            cassandraOperation.getRecordsByIndexedProperty(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(response1);
  }

  @Ignore
  public void testCheckExternalIdUniquenessForCreate() {
    try {
      Util.checkExternalIdUniqueness(user, JsonKey.CREATE);
    } catch (Exception ex) {
      System.out.println("1" + ex.getMessage());
      assertTrue(
          ex.getMessage()
              .equalsIgnoreCase(
                  "User already exists for given externalId (id: 123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf, idType: AAADHAR, provider: AP)."));
    }
  }

  // will try to delete other user extIds
  @Ignore
  public void testCheckExternalIdUniquenessForUpdate() {

    try {
      user.setUserId("456456");
      user.getExternalIds().get(0).put(JsonKey.OPERATION, JsonKey.DELETE);
      Util.checkExternalIdUniqueness(user, JsonKey.UPDATE);
    } catch (Exception ex) {
      System.out.println("2" + ex.getMessage());
      assertTrue(
          ex.getMessage()
              .equalsIgnoreCase(
                  "External ID (id: 123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf, idType: AAADHAR, provider: AP) not found for given user."));
    }
  }

  // will try to update other user extIds
  @Ignore
  public void testCheckExternalIdUniquenessForUpdate2() {

    try {
      user.setUserId("456456");
      user.getExternalIds().get(0).put(JsonKey.OPERATION, JsonKey.UPDATE);
      Util.checkExternalIdUniqueness(user, JsonKey.UPDATE);
    } catch (Exception ex) {
      System.out.println("3" + ex.getMessage());
      assertTrue(
          ex.getMessage()
              .equalsIgnoreCase(
                  "External ID (id: 123209-453445934-23128u3423-dsafsa32c43-few43-wesc49cjkf, idType: AAADHAR, provider: AP) not found for given user."));
    }
  }

  // will try to delete non existing extIds
  @Ignore
  public void testCheckExternalIdUniquenessForUpdate3() {

    try {

      Map<String, String> externalIdReqMap = new HashMap<>();
      externalIdReqMap.put(JsonKey.ID, "123209-453445934-23128u3423-dsafsa32c43-few43");
      externalIdReqMap.put(JsonKey.PROVIDER, "AP");
      externalIdReqMap.put(JsonKey.ID_TYPE, "PAN");
      externalIdReqMap.put(JsonKey.OPERATION, JsonKey.UPDATE);
      List<Map<String, String>> extIdList = new ArrayList<>();
      extIdList.add(externalIdReqMap);

      User user2 = new User();
      user2.setUserId("456456");
      user2.setExternalIds(extIdList);

      Util.checkExternalIdUniqueness(user2, JsonKey.UPDATE);
    } catch (Exception ex) {
      System.out.println("4" + ex.getMessage());
      assertTrue(
          ex.getMessage()
              .equalsIgnoreCase(
                  "External ID (id: 123209-453445934-23128u3423-dsafsa32c43-few43, idType: PAN, provider: AP) not found for given user."));
    }
  }
}
