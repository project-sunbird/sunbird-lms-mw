package org.sunbird.init.service.impl;

import java.io.IOException;
import org.sunbird.common.models.response.Response;
import org.sunbird.init.dao.SystemSettingDao;
import org.sunbird.init.dao.impl.SystemSettingDaoImpl;
import org.sunbird.init.model.SystemSetting;
import org.sunbird.init.service.SystemSettingService;

/** @author Loganathan */
public class SystemSettingServiceImpl implements SystemSettingService {
  private SystemSettingDao systemSettingDao;

  public SystemSettingServiceImpl() {
    this.systemSettingDao = new SystemSettingDaoImpl();
  }

  @Override
  public Response writeSetting(SystemSetting systemSetting) throws IOException {
    Response response = this.systemSettingDao.write(systemSetting);
    return response;
  }

  @Override
  public SystemSetting readSetting(String id) throws IOException {
    SystemSetting systemSetting = this.systemSettingDao.readById(id);
    return systemSetting;
  }
}
