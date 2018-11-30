package org.sunbird.learner.actors.bulkupload;

import static org.sunbird.common.exception.ProjectCommonException.throwClientErrorException;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.response.ResponseParams;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TextbookActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.ContentCloudStore;
import org.sunbird.content.textbook.TextBookTocUpload;
import org.sunbird.content.util.ContentStoreUtil;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
        validateTextBook(request);
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
        String textbookId = (String) request.get(JsonKey.TEXTBOOK_ID);
        Map<String, Object> readHierarchyResponse = ContentStoreUtil.readHierarchy(textbookId);
        Response response = new Response();
        String responseCode = (String) readHierarchyResponse.get(JsonKey.RESPONSE_CODE);
        if (StringUtils.equals(ResponseCode.OK.name(), responseCode)) {
            Map<String, Object> result  = (Map<String, Object>) readHierarchyResponse.get(JsonKey.RESULT);
            Map<String, Object> content = (Map<String, Object>) result.get(JsonKey.CONTENT);
            File csv = null;
            if (null != content) {
                String versionKey = (String) content.get(JsonKey.VERSION_KEY);
                String prefix =
                        File.separator + TextBookTocUpload.textBookTocCloudFolder + File.separator +
                                textbookId + "_" + versionKey + "." + JsonKey.FILE_TYPE_CSV;
                String cloudPath = ContentCloudStore.getUri(prefix, false);
                if (StringUtils.isBlank(cloudPath)) {
                    cloudPath = TextBookTocUpload.csvToCloud(csv, content, textbookId, versionKey);
                }
                Map<String, Object> textbook = new HashMap<>();
                textbook.put(JsonKey.TOC_URL, cloudPath);
                textbook.put(JsonKey.TTL, ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_CSV_TTL));
                response.put(JsonKey.TEXTBOOK, textbook);
            }
        } else {
            throw new ProjectCommonException(
                ResponseCode.errorProcessingRequest.getErrorCode(),
                ResponseCode.errorProcessingRequest.getErrorMessage(),
                ResponseCode.SERVER_ERROR.getResponseCode());
        }
        sender().tell(response, sender());
    }

    private void validateTextBook(Request request) {
        String mode = ((Map<String, Object>) request.get(JsonKey.DATA)).get(JsonKey.MODE).toString();
        Map<String, Object> response = ContentStoreUtil.readContent(request.get(JsonKey.TEXTBOOK_ID).toString());
        if (null != response && !response.isEmpty()) {
            Map<String, Object> result = (Map<String, Object>) response.get(JsonKey.RESULT);
            Map<String, Object> textbook = (Map<String, Object>) result.get(JsonKey.CONTENT);
            List<String> allowedContentTypes = Arrays.asList(ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_ALLOWED_CONTNET_TYPES).split(","));
            if (!JsonKey.TEXTBOOK_TOC_ALLOWED_MIMETYPE.equalsIgnoreCase(textbook.get(JsonKey.MIME_TYPE).toString()) || !allowedContentTypes.contains(textbook.get(JsonKey.CONTENT_TYPE).toString())) {
                throwClientErrorException(ResponseCode.invalidTextbook, ResponseCode.invalidTextbook.getErrorMessage());
            }
            if (JsonKey.CREATE.equalsIgnoreCase(mode)) {
                List<Object> children = textbook.containsKey(JsonKey.CHILDREN) ? (List<Object>) textbook.get(JsonKey.CHILDREN) : null;
                if (null != children || !children.isEmpty()) {
                    throwClientErrorException(ResponseCode.textbookChildrenExist, ResponseCode.textbookChildrenExist.getErrorMessage());
                }
            }
        } else {
            throw new ProjectCommonException(
                    ResponseCode.errorProcessingRequest.getErrorCode(),
                    ResponseCode.errorProcessingRequest.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        }
    }
}
