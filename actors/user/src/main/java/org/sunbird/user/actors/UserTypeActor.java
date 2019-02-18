package org.sunbird.user.actors;

import java.util.ArrayList;
import java.util.List;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.models.user.UserType;

@ActorConfig(
  tasks = {"getUserTypes"},
  asyncTasks = {}
)
public class UserTypeActor extends UserBaseActor {

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
    List<String> userTypeList = new ArrayList<>();

    for (UserType userType : UserType.values()) {
      userTypeList.add(userType.getTypeName());
    }
    return userTypeList;
  }
}
