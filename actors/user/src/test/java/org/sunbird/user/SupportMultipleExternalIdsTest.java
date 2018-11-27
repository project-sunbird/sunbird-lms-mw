package org.sunbird.user;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
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
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;

/** @author Amit Kumar */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ElasticSearchUtil.class,
  CassandraOperationImpl.class,
  ServiceFactory.class,
  EncryptionService.class,
  org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.class
})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class SupportMultipleExternalIdsTest {
  private static List<Map<String, String>> externalIds = new ArrayList<>();
  private static CassandraOperation cassandraOperation;
  private static User user = null;
  private static EncryptionService encryptionService;

  @Before
  public void beforeEach() throws Exception {

    PowerMockito.mockStatic(org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.class);
    encryptionService = Mockito.mock(EncryptionService.class);
    Mockito.when(
            org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                .getEncryptionServiceInstance(null))
        .thenReturn(encryptionService);
    Mockito.when(encryptionService.encryptData(Mockito.anyString())).thenReturn("abc123");
  }

  @BeforeClass
  public static void setUp() throws Exception {

    Map<String, String> externalIdReqMap1 = new HashMap<>();
    externalIdReqMap1.put(JsonKey.ID, "someId");
    externalIdReqMap1.put(JsonKey.PROVIDER, "SomeProvider");
    externalIdReqMap1.put(JsonKey.ID_TYPE, "someIdType");

    externalIds.add(externalIdReqMap1);
    user = new User();
    user.setExternalIds(externalIds);

    Map<String, String> externalIdResMap = new HashMap<>();
    externalIdResMap.put(JsonKey.PROVIDER, "someProvider");
    externalIdResMap.put(JsonKey.ID_TYPE, "someIdType");
    externalIdResMap.put(JsonKey.USER_ID, "someUserId");
    externalIdResMap.put(JsonKey.EXTERNAL_ID, "someExternalId");

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = PowerMockito.mock(CassandraOperationImpl.class);
    PowerMockito.when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    Response response1 = new Response();
    List<Map<String, String>> resMapList = new ArrayList<>();
    resMapList.add(externalIdResMap);
    response1.put(JsonKey.RESPONSE, resMapList);

    PowerMockito.when(
            cassandraOperation.getRecordsByCompositeKey(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(response1);
  }

  @Test
  public void testCheckExternalIdUniquenessForCreateFailure() {

    try {
      Util.checkExternalIdUniqueness(user, JsonKey.CREATE);
    } catch (ProjectCommonException e) {
      assertEquals(ResponseCode.userAlreadyExists.getErrorCode(), e.getCode());
    }
  }

  @Test
  public void testCheckExternalIdUniquenessForUpdate() {

    try {
      user.setUserId("abc123");
      Util.checkExternalIdUniqueness(user, JsonKey.UPDATE);
    } catch (ProjectCommonException e) {
      assertEquals(ResponseCode.externalIdAssignedToOtherUser.getErrorCode(), e.getCode());
    }
  }

  @Test
  public void testCheckExternalIdNotFoundForUpdate() {

    try {
      user.setUserId("abc123");
      user.getExternalIds().get(0).put(JsonKey.OPERATION, JsonKey.UPDATE);
      Util.checkExternalIdUniqueness(user, JsonKey.UPDATE);
    } catch (ProjectCommonException e) {
      assertEquals(ResponseCode.externalIdNotFound.getErrorCode(), e.getCode());
    }
  }

  @Test
  public void testCheckExternalIdNotFoundForDelete() {

    try {
      user.setUserId("abc123");
      user.getExternalIds().get(0).put(JsonKey.OPERATION, JsonKey.REMOVE);
      Util.checkExternalIdUniqueness(user, JsonKey.UPDATE);
    } catch (ProjectCommonException e) {
      assertEquals(ResponseCode.externalIdNotFound.getErrorCode(), e.getCode());
    }
  }
}
