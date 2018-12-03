package org.sunbird.learner.actors.bulkupload;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.response.ResponseParams;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TextbookActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.textbook.TextBookTocUploader;
import org.sunbird.content.util.ContentCloudStore;
import org.sunbird.content.util.ContentStoreUtil;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.sunbird.common.exception.ProjectCommonException.throwClientErrorException;
import static org.sunbird.content.textbook.FileType.Type.CSV;

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

    private void upload(Request request) throws Exception {
        String mode = ((Map<String, Object>) request.get(JsonKey.DATA)).get(JsonKey.MODE).toString();
        validateTextBook(request, mode);
        Response response = new Response();
        if(StringUtils.equalsIgnoreCase(mode, "create")){
            response = createTextbook(request);
        }else if(StringUtils.equalsIgnoreCase(mode, "update")) {
            response = updateTextbook(request);
        } else{
            unSupportedMessage();
        }
        sender().tell(response, sender());
    }

    private void getTocUrl(Request request) {
        String textbookId = (String) request.get(JsonKey.TEXTBOOK_ID);
        validateTocUrlRequest(textbookId);
        Map<String, Object> readHierarchyResponse = ContentStoreUtil.readHierarchy(textbookId);
        Response response = new Response();
        String responseCode = (String) readHierarchyResponse.get(JsonKey.RESPONSE_CODE);
        if (StringUtils.equals(ResponseCode.OK.name(), responseCode)) {
            Map<String, Object> result  = (Map<String, Object>) readHierarchyResponse.get(JsonKey.RESULT);

            Map<String, Object> content = (Map<String, Object>) result.get(JsonKey.CONTENT);
            if (null != content) {
                if (!StringUtils.equalsIgnoreCase(
                        JsonKey.TEXTBOOK, (String) content.get(JsonKey.CONTENT_TYPE)))
                    throw new ProjectCommonException(
                            ResponseCode.invalidTextBook.getErrorCode(),
                            ResponseCode.invalidTextBook.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());

                Object children = content.get(JsonKey.CHILDREN);
                if (null == children || ((List<Map>) children).isEmpty())
                    throw new ProjectCommonException(
                      ResponseCode.noChildrenExists.getErrorCode(),
                      ResponseCode.noChildrenExists.getErrorMessage(),
                      ResponseCode.CLIENT_ERROR.getResponseCode());

                String versionKey = (String) content.get(JsonKey.VERSION_KEY);
                String prefix =
                        TextBookTocUploader.textBookTocFolder + File.separator +
                                    textbookId + "_" + versionKey + CSV.getExtension();
                String cloudPath = ContentCloudStore.getUri(prefix, false);
                if (StringUtils.isBlank(cloudPath))
                    cloudPath = new TextBookTocUploader(null).execute(content, textbookId, versionKey);

                Map<String, Object> textbook = new HashMap<>();
                textbook.put(JsonKey.TOC_URL, cloudPath);
                textbook.put(JsonKey.TTL,
                        ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_CSV_TTL));
                response.put(JsonKey.TEXTBOOK, textbook);
            }
        } else {
            throw new ProjectCommonException(
                ResponseCode.textBookNotFound.getErrorCode(),
                ResponseCode.textBookNotFound.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        sender().tell(response, sender());
    }

    private void validateTextBook(Request request, String mode) {
        Map<String, Object> textbook = getTextbook((String) request.get(JsonKey.TEXTBOOK_ID));
        List<String> allowedContentTypes = Arrays.asList(ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_ALLOWED_CONTNET_TYPES).split(","));
        if (!JsonKey.TEXTBOOK_TOC_ALLOWED_MIMETYPE.equalsIgnoreCase(textbook.get(JsonKey.MIME_TYPE).toString()) || !allowedContentTypes.contains(textbook.get(JsonKey.CONTENT_TYPE).toString())) {
            throwClientErrorException(ResponseCode.invalidTextbook, ResponseCode.invalidTextbook.getErrorMessage());
        }
        if (JsonKey.CREATE.equalsIgnoreCase(mode)) {
            List<Object> children = textbook.containsKey(JsonKey.CHILDREN) ? (List<Object>) textbook.get(JsonKey.CHILDREN) : null;
            if (null != children && !children.isEmpty()) {
                throwClientErrorException(ResponseCode.textbookChildrenExist, ResponseCode.textbookChildrenExist.getErrorMessage());
            }
        }
    }

    private Response createTextbook(Request request) throws Exception {
        Map<String, Object> file = (Map<String, Object>) request.get(JsonKey.DATA);
        List<Map<String, Object>> data = (List<Map<String, Object>>) file.get(JsonKey.FILE_DATA);

        if(CollectionUtils.isEmpty(data)){
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    ResponseCode.invalidRequestData.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }else{
            String tbId = (String) request.get(JsonKey.TEXTBOOK_ID);
            Map<String, Object> tbMetadata = getTextbook(tbId);
            Map<String, Object> nodesModified = new HashMap<>();
            Map<String, Object> hierarchyData = new HashMap<>();
            hierarchyData.put(tbId, new HashMap<String, Object>() {{
                put(JsonKey.NAME, tbMetadata.get(JsonKey.NAME));
                put(JsonKey.CONTENT_TYPE, tbMetadata.get(JsonKey.CONTENT_TYPE));
                put(JsonKey.CHILDREN, new HashSet<>());
                put("root", true);
            }});
            for(Map<String, Object> row: data) {
                populateNodes(row, tbId, tbMetadata, nodesModified, hierarchyData);
            }
            Map<String, Object> updateRequest = new HashMap<String, Object>() {{
                put(JsonKey.REQUEST, new HashMap<String, Object>(){{
                    put(JsonKey.DATA, new HashMap<String, Object>(){{
                        put(JsonKey.NODES_MODIFIED, nodesModified);
                        put(JsonKey.HIERARCHY, hierarchyData);
                    }});
                }});
            }};
            return updateHierarchy(tbId, updateRequest);
        }
    }

    private void populateNodes(Map<String, Object> row, String tbId, Map<String, Object> tbMetadata, Map<String, Object> nodesModified, Map<String, Object> hierarchyData) {
        Map<String, Object> hierarchy = (Map<String, Object>) row.get(JsonKey.HIERARCHY);
        hierarchy.remove("Textbook");
        hierarchy.remove("identifier");
        String unitType = (String) tbMetadata.get("contentType") + "Unit";
        int levelCount = 0;
        String code = tbId;
        String parentCode = tbId;
        String name = (String) tbMetadata.get("name");
        for(int i = 1; i<= hierarchy.size(); i++){
            if(StringUtils.isNotBlank((String) hierarchy.get("L:"+ i))){
                name = (String) hierarchy.get("L:"+ i);
                code += name;
                levelCount +=1;
                if(i-1 > 0)
                    parentCode +=(String) hierarchy.get("L:"+ (i-1));
            }else{
                break;
            }
        }
        code = getCode(code);
        parentCode = getCode(parentCode);
        if(levelCount == 1) {
            parentCode = tbId;
        }
        if(null == nodesModified.get(code)) {
            String finalName = name;
            Map<String, Object> metadata = (Map<String, Object>) row.get("metadata");
            List<String> keywords = Arrays.asList(((String)metadata.get("keywords")).split(","));
            List<String> gradeLevel = Arrays.asList(((String)metadata.get("gradeLevel")).split(","));
            metadata.remove("keywords");
            metadata.remove("gradeLevel");
            nodesModified.put(code, new HashMap<String, Object>() {{
                put("isNew", true);
                put("root", false);
                put("metadata", new HashMap<String, Object>(){{
                    put("name", finalName);
                    put("mimeType", "application/vnd.ekstep.content-collection");
                    put("contentType", unitType);
                    put("framework", tbMetadata.get("framework"));
                    putAll(metadata);
                    put("keywords", keywords);
                    put("gradeLevel", gradeLevel);
                }});
            }});
        }

        if(null != hierarchyData.get(code)) {
            ((Map<String, Object>) hierarchyData.get(code)).put("name", name);
        }else{
            String finalName1 = name;
            hierarchyData.put(code, new HashMap<String, Object>() {{
                put(JsonKey.NAME, finalName1);
                put(JsonKey.CHILDREN, new HashSet<>());
                put("root", false);
            }});
        }

        if(null != hierarchyData.get(parentCode)){
            ((Set) ((Map<String, Object>) hierarchyData.get(parentCode)).get("children")).add(code);
        } else{
            String finalCode = code;
            hierarchyData.put(parentCode, new HashMap<String, Object>() {{
                put(JsonKey.NAME, "");
                put(JsonKey.CHILDREN, new HashSet<String>(){{
                    add(finalCode);
                }});
                put("root", false);
            }});
        }
    }

    private String getCode(String code) {
        return DigestUtils.md5Hex(code);
    }


    private Map<String, Object> getTextbook(String tbId) {
        Map<String, Object> response = ContentStoreUtil.readContent(tbId);
        if (null != response && !response.isEmpty() && StringUtils.equals(ResponseCode.OK.name(), (String)response.get(JsonKey.RESPONSE_CODE))) {
            Map<String, Object> result = (Map<String, Object>) response.get(JsonKey.RESULT);
            Map<String, Object> textbook = (Map<String, Object>) result.get(JsonKey.CONTENT);
            return textbook;
        } else {
            throw new ProjectCommonException(
                    ResponseCode.errorProcessingRequest.getErrorCode(),
                    ResponseCode.errorProcessingRequest.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        }
    }

    private Response updateTextbook(Request request) throws Exception {
        List<Map<String, Object>> data = (List<Map<String, Object>>) ((Map<String, Object>) request.get(JsonKey.DATA)).get(JsonKey.FILE_DATA);
        Map<String, Object> nodesModified = new HashMap<>();
        for(Map<String, Object> row : data) {
            Map<String, Object> metadata = (Map<String, Object>) row.get(JsonKey.METADATA);
            String id = (String) metadata.get(JsonKey.ID);
            List<String> keywords = Arrays.asList(((String)metadata.get("keywords")).split(","));
            List<String> gradeLevel = Arrays.asList(((String)metadata.get("gradeLevel")).split(","));
            metadata.remove(JsonKey.ID);
            metadata.remove("keywords");
            metadata.remove("gradeLevel");
            if(StringUtils.isNotBlank(id)){
                nodesModified.put(id, new HashMap<String, Object>(){{
                    put("isNew", false);
                    put("root", false);
                    put("metadata", new HashMap<String, Object>(){{
                        putAll(metadata);
                        put("keywords", keywords);
                        put("gradeLevel", gradeLevel);
                    }});

                }});
            }
        }

        Map<String, Object> updateRequest = new HashMap<String, Object>() {{
            put(JsonKey.REQUEST, new HashMap<String, Object>() {{
                put(JsonKey.DATA, new HashMap<String, Object>() {{
                    put(JsonKey.NODES_MODIFIED, nodesModified);
                }});
            }});
        }};
        return updateHierarchy((String) request.get(JsonKey.TEXTBOOK_ID), updateRequest);
    }

    private Response updateHierarchy(String tbId, Map<String, Object> updateRequest) throws Exception {
        String requestUrl =
                ProjectUtil.getConfigValue(JsonKey.SUNBIRD_API_BASE_URL)
                        + "/content/v3/hierarchy/update";


        String updateResp = HttpUtil.sendPatchRequest(requestUrl, mapper.writeValueAsString(updateRequest), getDefaultHeaders());

        if(StringUtils.equalsIgnoreCase(updateResp, ResponseCode.success.getErrorCode())){
            Response response = new Response();
            response.setResponseCode(ResponseCode.OK);
            ResponseParams params = new ResponseParams();
            params.setStatus("successful");
            response.setParams(params);
            Map<String, Object> result = new HashMap<>();
            result.put(JsonKey.CONTENT_ID,  tbId);
            response.putAll(result);
            return response;
        }else{
            throw new ProjectCommonException(
                    ResponseCode.errorProcessingRequest.getErrorCode(),
                    ResponseCode.errorProcessingRequest.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        }
    }

    private Map<String,String> getDefaultHeaders() {

        return  new HashMap<String, String>() {{
            put("Content-Type", "application/json");
            put(JsonKey.AUTHORIZATION, JsonKey.BEARER + ProjectUtil.getConfigValue(JsonKey.SUNBIRD_AUTHORIZATION));
        }};
    }

    private void validateTocUrlRequest(String textbookId){
        if (StringUtils.isBlank(textbookId))
            throw new ProjectCommonException(
                    ResponseCode.invalidTextBook.getErrorCode(),
                    ResponseCode.invalidTextBook.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
    }

}
