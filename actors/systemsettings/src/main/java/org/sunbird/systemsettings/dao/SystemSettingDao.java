package org.sunbird.systemsettings.dao;

import java.util.List;
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
   * This methods fetch the settings record using given id from cassandra table through
   * CassandraOperation methods
   *
   * @param field field of the settings record to be fetched
   * @return instance of SystemSetting class with mapped field values(id,field,value) from cassandra
   *     table
   */
  SystemSetting readByField(String field);

  /**
   * Read all system settings.
   *
   * @return Response containing list of system settings.
   */
  List<SystemSetting> readAll();
}
