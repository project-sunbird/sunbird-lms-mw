package org.sunbird.systemsettings.dao;

import org.sunbird.common.models.response.Response;
import org.sunbird.systemsettings.model.SystemSetting;

public interface SystemSettingDao {
  /**
   * Update system setting.
   *
   * @param systemSetting Setting information
   * @return Response containing setting identifier.
   */
  Response write(SystemSetting systemSetting);
  
  /**
   * Read system setting for given identifier.
   *
   * @param id System setting identifier
   * @return System setting information
   */
  SystemSetting readById(String id);
  
  /**
   * Read all system settings.
   *
   * @return Response containing list of system settings.
   */
  Response readAll();
}
