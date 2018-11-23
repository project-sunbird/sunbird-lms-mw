package org.sunbird.learner.actors.bulkupload;

import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.response.ResponseParams;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import java.util.HashMap;
import java.util.Map;

@ActorConfig(
        tasks = {"textbookTocUpload"},
        asyncTasks = {})
public class TextbookTocUploadActor extends BaseBulkUploadActor {

    @Override
    public void onReceive(Request request) throws Throwable {
        Response response = new Response();
        response.setResponseCode(ResponseCode.OK);
        ResponseParams params = new ResponseParams();
        params.setStatus("successful");
        response.setParams(params);
        Map<String, Object> result = new HashMap<>();
        result.put("node_id", "do_11263298042220544013");
        result.put("versionKey", "1542273096671");
        response.putAll(result);
        sender().tell(response, sender());
    }
}