package org.sunbird.user.actors;

import java.util.List;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.user.dao.UserTypeDao;
import org.sunbird.user.dao.impl.UserTypeDaoImpl;

@ActorConfig(
  tasks = {"getUserTypes"},
  asyncTasks = {}
)
public class UserTypeActor extends UserBaseActor {

  private UserTypeDao userTypeDao = UserTypeDaoImpl.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "getUserTypes":
        getUserTypes();
        break;
    }
  }

  @SuppressWarnings("unchecked")
  private void getUserTypes() {

    Response response = new Response();
    List<String> userTypeList = getUserTypeList();
    response.getResult().put(JsonKey.USER_TYPES, userTypeList);
    sender().tell(response, self());
  }

  private List<String> getUserTypeList() {
    List<String> userTypeList = null;
    try {
      Response res = userTypeDao.getUserTypes();
      if (!((List<String>) res.get(JsonKey.RESPONSE)).isEmpty()) {
        userTypeList = (List<String>) res.get(JsonKey.RESPONSE);
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return userTypeList;
  }
}
