package org.sunbird.user.actors;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.bean.ShadowUser;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.user.util.MigrationUtils;


@ActorConfig(
        tasks = {"migrateUser","rejectMigration"},
        asyncTasks = {}
)
public class MigrationActor extends BaseActor {

    @Override
    public void onReceive(Request request) throws Throwable {
        if(StringUtils.equalsIgnoreCase(ActorOperations.REJECT_MIGRATION.getValue(),request.getOperation())){
            rejectMigration(request);
        }
    }

    private void rejectMigration(Request request) {
        String userId = (String) request.getContext().get(JsonKey.USER_ID);
        ProjectLogger.log("MigrationActor:rejectMigration: started rejecting Migration with userId:"+userId, LoggerEnum.INFO.name());
        if (StringUtils.isBlank(userId)) {
            ProjectCommonException.throwClientErrorException(ResponseCode.invalidUserId);
        }
        ShadowUser shadowUser = MigrationUtils.getRecordByUserId(userId);
        if (null == shadowUser) {
            ProjectCommonException.throwClientErrorException(ResponseCode.invalidUserId);
        }
        MigrationUtils.markUserAsRejected(shadowUser);
        Response response = new Response();
        response.put(JsonKey.SUCCESS, true);
        sender().tell(response, self());
    }
}