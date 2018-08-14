package org.sunbird.learner.actors.skill;

import java.text.SimpleDateFormat;
import java.util.*;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.skill.dao.UserSkillDao;
import org.sunbird.learner.actors.skill.dao.impl.UserSkillDaoImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.skill.Skill;
import org.sunbird.telemetry.util.TelemetryUtil;

@ActorConfig(
  tasks = {"addUserSkillEndorsement"},
  asyncTasks = {}
)
public class UserSkillEndorsementManagementActor extends BaseActor {
  private UserSkillDao userSkillDao = UserSkillDaoImpl.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    Util.initializeContext(request, TelemetryEnvKey.USER);
    // set request id fto thread loacl...
    ExecutionContext.setRequestId(request.getRequestId());

    switch (operation) {
      case "addUserSkillEndorsement":
        endorseUserSkill(request);
        break;
      default:
        onReceiveUnsupportedOperation("UserSkillEndorsementManagementActor");
    }
  }

  private void endorseUserSkill(Request request) {
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    Map<String, Object> targetObject;

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Skill skill = userSkillDao.read((String) request.getRequest().get("skillId"));
    String endorsersId = (String) request.getRequest().get(JsonKey.USER_ID);
    String endorsedId = (String) request.getRequest().get(JsonKey.ENDORSED_USER_ID);
    List<HashMap<String, String>> endorsersList = skill.getEndorsersList();
    HashMap<String, String> endoresers = new HashMap<>();
    if (endorsersList.isEmpty()) {
      endoresers.put(JsonKey.USER_ID, endorsersId);
      endoresers.put(JsonKey.ENDORSE_DATE, format.format(new Date()));
      endorsersList.add(endoresers);
      skill.setEndorsementCount(0);
    } else {
      boolean flag = false;
      for (Map<String, String> map : endorsersList) {
        if ((map.get(JsonKey.USER_ID)).equalsIgnoreCase(endorsersId)) {
          flag = true;
          break;
        }
      }
      if (flag) {
        // donot do anything..
        ProjectLogger.log(endorsersId + " has already endorsed the " + endorsedId);
      } else {
        Integer endoresementCount = skill.getEndorsementCount() + 1;
        endoresers.put(JsonKey.USER_ID, endorsersId);
        endoresers.put(JsonKey.ENDORSE_DATE, format.format(new Date()));
        endorsersList.add(endoresers);
        skill.setEndorsementCount(endoresementCount);
      }
    }
    skill.setEndorsersList(endorsersList);

    userSkillDao.update(skill);
    Response response = new Response();
    response.getResult().put(JsonKey.RESULT, "SUCCESS");
    sender().tell(response, self());

    targetObject =
        TelemetryUtil.generateTargetObject(endorsersId, JsonKey.USER, JsonKey.UPDATE, null);
    TelemetryUtil.generateCorrelatedObject(endorsersId, JsonKey.USER, null, correlatedObject);
    TelemetryUtil.telemetryProcessingCall(request.getRequest(), targetObject, correlatedObject);
  }
}
