package org.sunbird.systemsettings.dao.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.systemsettings.dao.SystemSettingDao;
import org.sunbird.systemsettings.model.SystemSetting;

/**
 * This class implements the test cases for system settings DAO
 *
 * @author Loganathan
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CassandraOperationImpl.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class SystemSettingDaoImplTest {
  private CassandraOperation mockDBService;
  private SystemSettingDao systemSettingDaoImpl;

  @Before
  public void setUp() {
    mockDBService = PowerMockito.mock(CassandraOperationImpl.class);
    systemSettingDaoImpl = new SystemSettingDaoImpl(mockDBService);
  }

  @Test
  public void testSaveSuccess() {
    PowerMockito.when(mockDBService.upsertRecord(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(new Response());
    boolean thrown = false;
    try {
      SystemSetting systemSetting =
          new SystemSetting("defaultRootOrgId", "defaultRootOrgId", "0125656442955776000");
      Response upsertResp = systemSettingDaoImpl.upsert(systemSetting);
      Assert.assertNotEquals(null, upsertResp);
    } catch (Exception e) {
      thrown = true;
    }
    Assert.assertEquals(false, thrown);
  }

  @Test
  public void testReadByIdSuccess() {
    Response response = new Response();
    response.put(
        JsonKey.RESPONSE, new ArrayList<Map<String, Object>>(Arrays.asList(new HashMap<>())));

    PowerMockito.when(
            mockDBService.getRecordById(Mockito.any(), Mockito.any(), Mockito.anyString()))
        .thenReturn(response);

    SystemSetting systemSetting = systemSettingDaoImpl.readById("defaultRootOrgId");
    Assert.assertTrue(null != systemSetting);
  }

  @Test
  public void testReadAllSuccess() {
    Response response = new Response();
    response.put(
        JsonKey.RESPONSE, new ArrayList<Map<String, Object>>(Arrays.asList(new HashMap<>())));

    PowerMockito.when(mockDBService.getAllRecords(Mockito.any(), Mockito.any()))
        .thenReturn(response);

    Response result = systemSettingDaoImpl.readAll();
    Assert.assertTrue(null != result);
  }

  @Test
  public void testReadByIdEmpty() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, new ArrayList());

    PowerMockito.when(
            mockDBService.getRecordById(Mockito.any(), Mockito.any(), Mockito.anyString()))
        .thenReturn(response);

    SystemSetting systemSetting = systemSettingDaoImpl.readById("defaultRootOrgId");
    Assert.assertTrue(null == systemSetting);
  }

  @Test
  public void testReadAllEmpty() {
    PowerMockito.when(mockDBService.getAllRecords(Mockito.any(), Mockito.any())).thenReturn(null);

    Response result = systemSettingDaoImpl.readAll();
    Assert.assertTrue(null == result);
  }
}
