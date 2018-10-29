package org.sunbird.user.dao;

import org.sunbird.user.dao.impl.EducationDaoImpl;

public class EducationFactory {

  private static EducationDao educationDao = null;

  private EducationFactory() {}

  public static EducationDao getInstance() {
    if (null == educationDao) {
      educationDao = new EducationDaoImpl();
    }
    return educationDao;
  }
}
