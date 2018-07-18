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
   * This method will write the specific setting
   *
   * @param request Request
   * @exception IOException
   * @return Response
   */
  public Response writeSetting(SystemSetting systemSetting) throws IOException;

  /**
   * This method will read the specific setting
   *
   * @param request Request
   * @exception IOException
   * @return Response
   */
  public SystemSetting readSetting(String id) throws IOException;
}
