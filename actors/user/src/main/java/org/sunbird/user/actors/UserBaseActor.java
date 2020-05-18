package org.sunbird.user.actors;

import akka.actor.ActorRef;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.request.UserRequestValidator;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.user.dao.UserDao;
import org.sunbird.user.dao.impl.UserDaoImpl;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;

public abstract class UserBaseActor extends BaseActor {

  private UserRequestValidator userRequestValidator = new UserRequestValidator();
  private ActorRef systemSettingActorRef;
  private SSOManager ssoManager = SSOServiceFactory.getInstance();
  private UserDao userDao = UserDaoImpl.getInstance();
  private UserService userService = UserServiceImpl.getInstance();



  protected ActorRef getSystemSettingActorRef() {
    if (systemSettingActorRef == null) {
      systemSettingActorRef = getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue());
    }

    return systemSettingActorRef;
  }

  protected SSOManager getSSOManager() {
    return ssoManager;
  }

  protected UserRequestValidator getUserRequestValidator() {
    return userRequestValidator;
  }

  protected UserDao getUserDao() {
    return userDao;
  }

  public UserService getUserService() {
    return userService;
  }
}
