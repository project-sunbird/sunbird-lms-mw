package org.sunbird.user.dao;

import java.util.Map;

import org.sunbird.models.user.User;
import org.sunbird.user.dao.impl.UserDaoImpl;

import com.fasterxml.jackson.databind.ObjectMapper;

public class UserDaoFactory {

  private static UserDao userDao;

  private UserDaoFactory() {
    // private constructor - to disallow accidental instance creation
  }

  public static UserDao getUserDao() {
    if (userDao == null) {
      synchronized (UserDaoFactory.class) {
        if (userDao == null) {
          userDao = new UserDaoImpl();
        }
      }
    }
    return userDao;
  }
  
  public static User toUser(Map userMap) {
	  ObjectMapper mapper = new ObjectMapper();
	  return mapper.convertValue(userMap, User.class);
  }
}
