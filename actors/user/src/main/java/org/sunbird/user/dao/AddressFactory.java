package org.sunbird.user.dao;

import org.sunbird.user.dao.impl.AddressDaoImpl;

public class AddressFactory {

  private static AddressDao addressDao = null;

  private AddressFactory() {}

  public static AddressDao getInstance() {
    if (null == addressDao) {
      addressDao = new AddressDaoImpl();
    }
    return addressDao;
  }
}
