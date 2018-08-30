package org.sunbird.systemsettings.service.impl;

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
import org.sunbird.systemsettings.model.SystemSetting;
import org.sunbird.systemsettings.service.SystemSettingService;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CassandraOperationImpl.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class SystemSettingServiceImplTest {
  private CassandraOperation mockDBService;
  private SystemSettingService systemSettingServiceImpl;

  @Before
  public void setUp() {
    mockDBService = PowerMockito.mock(CassandraOperationImpl.class);
    systemSettingServiceImpl = new SystemSettingServiceImpl(mockDBService);
  }

  @Test
  public void testSetSetting() {
    SystemSetting systemSetting =
        new SystemSetting("defaultRootOrgId", "defaultRootOrgId", "0125656442955776000");
    PowerMockito.when(mockDBService.upsertRecord(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(new Response());
    boolean thrown = false;
    try {
      Response result = systemSettingServiceImpl.setSetting(systemSetting);
      Assert.assertTrue(null != result);
    } catch (Exception e) {
      thrown = true;
    }
    Assert.assertEquals(false, thrown);
  }

  @Test
  public void testReadSetting() {
    Response response = new Response();
    response.put(
        JsonKey.RESPONSE, new ArrayList<Map<String, Object>>(Arrays.asList(new HashMap<>())));
    PowerMockito.when(
            mockDBService.getRecordById(Mockito.any(), Mockito.any(), Mockito.anyString()))
        .thenReturn(response);
    boolean thrown = false;
    try {
      SystemSetting result = systemSettingServiceImpl.readSetting("defaultRootOrgId");
      Assert.assertTrue(null != result);
    } catch (Exception e) {
      thrown = true;
    }
    Assert.assertEquals(false, thrown);
  }

  @Test
  public void testReadSettingEmpty() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, new ArrayList());
    PowerMockito.when(
            mockDBService.getRecordById(Mockito.any(), Mockito.any(), Mockito.anyString()))
        .thenReturn(response);
    boolean thrown = false;
    try {
      SystemSetting result = systemSettingServiceImpl.readSetting("defaultRootOrgId");
      Assert.assertTrue(null == result);
    } catch (Exception e) {
      thrown = true;
    }
    Assert.assertEquals(false, thrown);
  }

  @Test
  public void testReadAllSettings() {
    Response response = new Response();
    response.put(
        JsonKey.RESPONSE, new ArrayList<Map<String, Object>>(Arrays.asList(new HashMap<>())));
    PowerMockito.when(mockDBService.getAllRecords(Mockito.any(), Mockito.any()))
        .thenReturn(response);
    boolean thrown = false;
    try {
      Response result = systemSettingServiceImpl.readAllSettings();
      Assert.assertTrue(null != result);
    } catch (Exception e) {
      thrown = true;
    }
    Assert.assertEquals(false, thrown);
  }
}
