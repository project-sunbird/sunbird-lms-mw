package org.sunbird.learner.actors.bulkupload;

import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.response.ResponseParams;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.TextbookActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import java.util.HashMap;
import java.util.Map;

@ActorConfig(tasks = {"textbookTocUpload", "textbookTocUrl", "textbookTocUpdate"}, asyncTasks = {})
public class TextbookTocActor extends BaseBulkUploadActor {

    @Override
    public void onReceive(Request request) throws Throwable {
        if (request.getOperation().equalsIgnoreCase(TextbookActorOperation.TEXTBOOK_TOC_UPLOAD.getValue())) {
            upload(request);
        } else if (request.getOperation().equalsIgnoreCase(TextbookActorOperation.TEXTBOOK_TOC_URL.getValue())) {
            getTocUrl(request);
        } else {
            onReceiveUnsupportedOperation(request.getOperation());
        }
    }

    private void upload(Request request) {
        Response response = new Response();
        response.setResponseCode(ResponseCode.OK);
        ResponseParams params = new ResponseParams();
        params.setStatus("successful");
        response.setParams(params);
        Map<String, Object> result = new HashMap<>();
        result.put("contentId", "do_11263298042220544013");
        result.put("versionKey", "1542273096671");
        response.putAll(result);
        sender().tell(response, sender());
    }

    private void getTocUrl(Request request) {
        Response response = new Response();
        Map<String, Object> textbook = new HashMap<>();
        textbook.put("tocUrl", "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1126441512460369921103/artifact/1_1543475510769.pdf");
        textbook.put("ttl", 86400);
        response.getResult().put(JsonKey.TEXTBOOK, textbook);
        sender().tell(response, sender());
    }

}
