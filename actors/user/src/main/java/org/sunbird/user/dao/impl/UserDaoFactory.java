package org.sunbird.user.dao.impl;

import org.sunbird.user.dao.UserDao;

/**
 * Factory of UserDao.
 * 
 * @author Amit Kumar
 *
 */
public class UserDaoFactory {

  /** private default constructor. */
  private UserDaoFactory(){}
  
  private static UserDao userDao = null;
  
  static{
    userDao =  new UserDaoImpl();
  }
  
  /**
   * This method will provide singleton instance for UserDaoImpl.
   *
   * @return UserDao
   */
  public static UserDao getInstance(){
    return userDao;
  }
}
