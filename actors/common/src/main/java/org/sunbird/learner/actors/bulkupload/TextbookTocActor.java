package org.sunbird.learner.actors.bulkupload;

import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.response.ResponseParams;
import org.sunbird.common.models.util.TextbookActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@ActorConfig(tasks = {"textbookTocUpload", "textbookTocDownload"}, asyncTasks = {})
public class TextbookTocActor extends BaseBulkUploadActor {

    @Override
    public void onReceive(Request request) throws Throwable {
        if (request.getOperation().equalsIgnoreCase(TextbookActorOperation.TEXTBOOK_TOC_UPLOAD.getValue())) {
            upload(request);
        } else if (request.getOperation().equalsIgnoreCase(TextbookActorOperation.TEXTBOOK_TOC_DOWNLOAD.getValue())) {
            download(request);
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

    private void download(Request request) throws Exception {
        File file = new File("test.csv");
        file.createNewFile();
        sender().tell(file, sender());
    }

}
