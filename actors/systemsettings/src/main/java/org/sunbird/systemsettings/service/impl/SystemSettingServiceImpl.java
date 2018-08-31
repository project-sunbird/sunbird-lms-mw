package org.sunbird.systemsettings.service.impl;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.systemsettings.dao.SystemSettingDao;
import org.sunbird.systemsettings.dao.impl.SystemSettingDaoImpl;
import org.sunbird.systemsettings.model.SystemSetting;
import org.sunbird.systemsettings.service.SystemSettingService;

public class SystemSettingServiceImpl implements SystemSettingService {
  private SystemSettingDao systemSettingDao;

  public SystemSettingServiceImpl(CassandraOperation cassandraOperation) {
    this.systemSettingDao = new SystemSettingDaoImpl(cassandraOperation);
  }
  /**
   * This methods writes the setting to the System settings
   *
   * @param systemSetting instance of SystemSetting class has the values to be written
   *     (id,field,value)
   * @return returns the instance of Reponse class with 'id' of created record
   */
  public Response setSetting(SystemSetting systemSetting) {
    Response response = this.systemSettingDao.write(systemSetting);
    return response;
  }
  /**
   * This methods reads the setting from System settings by its id
   *
   * @param id id of the setting to be fetched from system settings
   * @return returns the instance of SystemSetting class with elements id,field,value
   */
  public SystemSetting readSetting(String id) {
    SystemSetting systemSetting = this.systemSettingDao.readById(id);
    return systemSetting;
  }
  /**
   * This methods reads all the settings from System settings
   *
   * @return returns the instance of Response class with settings elements list
   */
  public Response readAllSettings() {
    Response response = this.systemSettingDao.readAll();
    return response;
  }
}
