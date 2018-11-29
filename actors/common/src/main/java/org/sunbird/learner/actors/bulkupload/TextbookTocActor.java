package org.sunbird.learner.actors.bulkupload;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.response.ResponseParams;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.TextbookActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.ContentCloudStore;
import org.sunbird.content.textbook.TextBookTocUpload;
import org.sunbird.content.util.ContentStoreUtil;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@ActorConfig(tasks = {"textbookTocUpload", "textbookTocUrl"}, asyncTasks = {})
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
//        String textbookId = (String) request.get(JsonKey.TEXTBOOK_ID);
//        Map<String, Object> readHierarchyResponse = ContentStoreUtil.readHierarchy(textbookId);
//        Response response = new Response();
//        String responseCode = (String) readHierarchyResponse.get(JsonKey.RESPONSE_CODE);
//        if (StringUtils.equals(ResponseCode.OK.name(), responseCode)) {
//            Map<String, Object> result  = (Map<String, Object>) readHierarchyResponse.get(JsonKey.RESULT);
//            Map<String, Object> content = (Map<String, Object>) result.get(JsonKey.CONTENT);
//            File csv = null;
//            if (null != content) {
//                String versionKey = (String) content.get(JsonKey.VERSION_KEY);
//                String prefix = File.separator + TextBookTocUpload.textBookTocCloudFolder + File.separator + textbookId + "_" + versionKey + ".csv";
//                String cloudPath = ContentCloudStore.getUri(prefix, false);
//                if (StringUtils.isBlank(cloudPath)) {
//                    cloudPath = TextBookTocUpload.csvToCloud(csv, content, textbookId, versionKey);
//                }
//                Map<String, Object> textbook = new HashMap<>();
//                textbook.put("tocUrl", cloudPath);
//                textbook.put("ttl", 86400);
//                response.put(JsonKey.TEXTBOOK, textbook);
//            }
//        } else {
//            ResponseParams params = new ResponseParams();
//            params.setStatus("failed");
//            params.setErr("ERR_GETTING_UPLOAD_FILE_URL");
//            params.setErrmsg("Error Getting TextBook Toc Url");
//            response.setParams(params);
//            response.setResponseCode(ResponseCode.SERVER_ERROR);
//            response.getResult().putAll((Map<String, Object>) readHierarchyResponse.get("result"));
//        }
        Response response = new Response();
        Map<String, Object> textbook = new HashMap<>();
        textbook.put("tocUrl", "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1126441512460369921103/artifact/1_1543475510769.pdf");
        textbook.put("ttl", 86400);
        response.getResult().put(JsonKey.TEXTBOOK, textbook);
        sender().tell(response, sender());
    }

}
