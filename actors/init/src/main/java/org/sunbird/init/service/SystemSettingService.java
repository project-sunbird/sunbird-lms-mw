package org.sunbird.init.service;

import java.io.IOException;
import org.sunbird.common.models.response.Response;
import org.sunbird.init.model.SystemSetting;

/**
 * This interface will have all the methods for System Settings.
 *
 * @author Loganathan
 */
public interface SystemSettingService {

  /**
   * This methods writes the setting to the System settings
   *
   * @param systemSetting instance of SystemSetting class has the values to be written
   *     (id,field,value)
   * @return returns the instance of Reponse class with 'id' of created record
   */
  public Response writeSetting(SystemSetting systemSetting) throws IOException;

  /**
   * This methods reads the setting from System settings by its id
   *
   * @param id id of the setting to be fetched from system settings
   * @return returns the instance of SystemSetting class with elements id,field,value
   */
  public SystemSetting readSetting(String id) throws IOException;
}
