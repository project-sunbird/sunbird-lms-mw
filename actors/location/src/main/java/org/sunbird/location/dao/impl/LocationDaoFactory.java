package org.sunbird.location.dao.impl;

import org.sunbird.location.dao.LocationDao;

public class LocationDaoFactory {

  private LocationDaoFactory() {}

  private static LocationDao locationDao;

  static {
    locationDao = new LocationDaoImpl();
  }

  public static LocationDao getInstance() {
    return locationDao;
  }
}
