package org.sunbird.init.dao;

import org.sunbird.common.models.response.Response;
import org.sunbird.init.model.SystemSetting;

/** @author Loganathan */
public interface SystemSettingDao {

  /**
   * @param systemSetting instance of SystemSetting class contains the setting to be written
   * @return response instance of Response class contains the response of cassandra Dao insert
   *     operation
   */
  Response write(SystemSetting systemSetting);

  /**
   * @param id Id of the setting to be read
   * @return systemSetting instance of SystemSetting model with settings details like id,field,value
   */
  SystemSetting readById(String id);
}
