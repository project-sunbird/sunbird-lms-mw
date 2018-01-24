package org.sunbird.learner.util;

import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;

public class ChannelRegHandler implements Runnable {

  @Override
  public void run() {
    ProjectLogger.log("Channel Registration handler started..");
    try {
      Thread.sleep(900000);
    } catch (Exception e) {
      ProjectLogger.log(e.getLocalizedMessage(), e);
    }
    startChannelRegProcess();
  }

  private void startChannelRegProcess() {
    ProjectLogger.log("startChannelRegProcess method call ");
    Request request = new Request();
    request.setOperation(ActorOperations.REG_CHANNEL.getValue());
    CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    Response response = cassandraOperation.getRecordById(JsonKey.SUNBIRD,
        JsonKey.SYSTEM_SETTINGS_DB, JsonKey.CHANNEL_REG_STATUS_ID);
    List<Map<String, Object>> responseList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (null != responseList && !responseList.isEmpty()) {
      Map<String, Object> resultMap = responseList.get(0);
      if (ProjectUtil.isStringNullOREmpty((String) resultMap.get(JsonKey.VALUE))
          && !Boolean.parseBoolean((String) resultMap.get(JsonKey.VALUE))) {
        ProjectLogger.log("calling ChannelRegistrationActor from startChannelRegProcess method.");
        ActorUtil.tell(request);
      }
    } else {
      ProjectLogger.log("calling ChannelRegistrationActor from startChannelRegProcess method.");
      ActorUtil.tell(request);
    }
  }
}
