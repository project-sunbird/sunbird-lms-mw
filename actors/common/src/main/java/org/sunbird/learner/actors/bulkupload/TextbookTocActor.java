package org.sunbird.learner.actors.bulkupload;


import com.fasterxml.jackson.core.type.TypeReference;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.jclouds.json.Json;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TextbookActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.textbook.TextBookTocUploader;
import org.sunbird.content.util.ContentCloudStore;
import org.sunbird.content.util.TextBookTocUtil;

import java.io.IOException;
import java.util.*;

import static java.io.File.separator;
import static org.sunbird.common.exception.ProjectCommonException.throwClientErrorException;
import static org.sunbird.common.models.util.JsonKey.*;
import static org.sunbird.common.responsecode.ResponseCode.OK;
import static org.sunbird.common.responsecode.ResponseCode.invalidTextbook;
import static org.sunbird.common.responsecode.ResponseCode.noChildrenExists;
import static org.sunbird.common.responsecode.ResponseCode.textBookNotFound;
import static org.sunbird.common.responsecode.ResponseCode.textbookChildrenExist;
import static org.sunbird.content.textbook.FileType.Type.CSV;
import static org.sunbird.content.textbook.TextBookTocUploader.textBookTocFolder;

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
        validateRequest(request, mode);
        Response response = new Response();
        if (StringUtils.equalsIgnoreCase(mode, "create")) {
            response = createTextbook(request);
        } else if (StringUtils.equalsIgnoreCase(mode, "update")) {
            response = updateTextbook(request);
        } else {
            unSupportedMessage();
        }
        sender().tell(response, sender());
    }

    private void getTocUrl(Request request) {
        String textbookId = (String) request.get(TEXTBOOK_ID);
        if (StringUtils.isBlank(textbookId)) {
            ProjectLogger.log("Invalid TextBook Provided");
            throwClientErrorException(invalidTextbook, invalidTextbook.getErrorMessage());
        }
        ProjectLogger.log("Reading Hierarchy for TextBook | Id: ", textbookId);
        Map<String, Object> readHierarchyResponse = TextBookTocUtil.readHierarchy(textbookId);
        Response response = new Response();
        String responseCode = (String) readHierarchyResponse.get(RESPONSE_CODE);
        if (StringUtils.equals(OK.name(), responseCode)) {
            Map<String, Object> result = (Map<String, Object>) readHierarchyResponse.get(RESULT);
            Map<String, Object> content = (Map<String, Object>) result.get(CONTENT);
            if (null != content) {
                validateTextBook(content, DOWNLOAD);
                String versionKey = (String) content.get(VERSION_KEY);
                String prefix =
                        textBookTocFolder + separator +
                                textbookId + "_" + versionKey + CSV.getExtension();
                ProjectLogger.log("Fetching TextBook Toc URL from Cloud", LoggerEnum.INFO);
                String cloudPath = ContentCloudStore.getUri(prefix, false);
                if (StringUtils.isBlank(cloudPath)) {
                    cloudPath = new TextBookTocUploader(null).execute(content, textbookId, versionKey);
                }
                ProjectLogger.log("Sending Response for Toc Download API for TextBook | Id: " + textbookId);
                Map<String, Object> textbook = new HashMap<>();
                textbook.put(TOC_URL, cloudPath);
                textbook.put(TTL,
                        ProjectUtil.getConfigValue(TEXTBOOK_TOC_CSV_TTL));
                response.put(TEXTBOOK, textbook);
            } else {
                ProjectLogger.log("No content fetched for TextBook | Id:" + textbookId, LoggerEnum.INFO);
            }
        } else {
            ProjectLogger.log("Error while fetching textbook : " + textbookId + " with response " + response, LoggerEnum.ERROR.name());
            throwClientErrorException(textBookNotFound, textBookNotFound.getErrorMessage());
        }
        sender().tell(response, sender());
    }


    private void validateTextBook(Map<String, Object> textbook, String mode) {
        List<String> allowedContentTypes = Arrays.asList(ProjectUtil.getConfigValue(TEXTBOOK_TOC_ALLOWED_CONTNET_TYPES).split(","));
        if (!TEXTBOOK_TOC_ALLOWED_MIMETYPE.equalsIgnoreCase(textbook.get(MIME_TYPE).toString()) || !allowedContentTypes.contains(textbook.get(CONTENT_TYPE).toString())) {
            throwClientErrorException(invalidTextbook, invalidTextbook.getErrorMessage());
        }
        List<Object> children = textbook.containsKey(JsonKey.CHILDREN) ? (List<Object>) textbook.get(JsonKey.CHILDREN) : null;
        if (JsonKey.CREATE.equalsIgnoreCase(mode)) {
            if (null != children && !children.isEmpty()) {
                throwClientErrorException(textbookChildrenExist, textbookChildrenExist.getErrorMessage());
            }
        } else if (DOWNLOAD.equalsIgnoreCase(mode)) {
            if (null == children || children.isEmpty())
                throwClientErrorException(noChildrenExists, noChildrenExists.getErrorMessage());
        }
    }

    private void validateRequest(Request request, String mode) throws IOException {
        Set<String> rowsHash = new HashSet<>();
        String mandatoryFields = ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_MANDATORY_FIELDS);
        Map<String, String> mandatoryFieldsMap = mapper.readValue(mandatoryFields, new TypeReference<Map<String, String>>() {
        });
        Map<String, Object> textbook = getTextbook((String) request.get(TEXTBOOK_ID));
        String textbookName = (String) textbook.get(JsonKey.NAME);

        validateTextBook(textbook, mode);

        List<Map<String, Object>> fileData = (List<Map<String, Object>>) ((Map<String, Object>) request.get(JsonKey.DATA)).get(JsonKey.FILE_DATA);


        for (int i = 0; i < fileData.size(); i++){
            Map<String, Object> row = fileData.get(i);
            Boolean isAdded = rowsHash.add(DigestUtils.md5Hex(SerializationUtils.serialize(row.toString())));
            if (!isAdded) {
                throwClientErrorException(ResponseCode.duplicateRows, ResponseCode.duplicateRows.getErrorMessage() + (i+1));
            }
            Map<String, Object> hierarchy = (Map<String, Object>) row.get(JsonKey.HIERARCHY);

            String name = (String) hierarchy.getOrDefault(StringUtils.capitalize(JsonKey.TEXTBOOK), "");
            if (StringUtils.isBlank(name) || !StringUtils.equalsIgnoreCase(name, textbookName)) {
                ProjectLogger.log("Name mismatch. Content has: "+ name + " but, file has: "+ textbookName, null, LoggerEnum.ERROR.name());
                throwClientErrorException(ResponseCode.invalidTextbookName, ResponseCode.invalidTextbookName.getErrorMessage());
            }
            for (String field : mandatoryFieldsMap.keySet()) {
                if (!hierarchy.containsKey(field) || StringUtils.isBlank(hierarchy.getOrDefault(field, "").toString())) {
                    throwClientErrorException(ResponseCode.requiredFieldMissing, ResponseCode.requiredFieldMissing.getErrorMessage() + mandatoryFieldsMap.values());
                }
            }
        }
    }

    private Response createTextbook(Request request) throws Exception {
        ProjectLogger.log("Create Textbook called ", LoggerEnum.INFO.name());
        Map<String, Object> file = (Map<String, Object>) request.get(JsonKey.DATA);
        List<Map<String, Object>> data = (List<Map<String, Object>>) file.get(JsonKey.FILE_DATA);
        ProjectLogger.log("Create Textbook - UpdateHierarchy input data : " + mapper.writeValueAsString(data));
        if (CollectionUtils.isEmpty(data)) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    ResponseCode.invalidRequestData.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        } else {
            String tbId = (String) request.get(TEXTBOOK_ID);
            Map<String, Object> tbMetadata = getTextbook(tbId);
            Map<String, Object> nodesModified = new HashMap<>();
            Map<String, Object> hierarchyData = new HashMap<>();
            nodesModified.put(tbId, new HashMap<String, Object>(){{
                put(JsonKey.TB_IS_NEW, false);
                put(JsonKey.TB_ROOT, true);
                put(JsonKey.METADATA, new HashMap<String, Object>());
            }});

            hierarchyData.put(tbId, new HashMap<String, Object>() {{
                put(JsonKey.NAME, tbMetadata.get(JsonKey.NAME));
                put(CONTENT_TYPE, tbMetadata.get(CONTENT_TYPE));
                put(JsonKey.CHILDREN, new ArrayList<>());
                put(JsonKey.TB_ROOT, true);
            }});
            for (Map<String, Object> row : data) {
                populateNodes(row, tbId, tbMetadata, nodesModified, hierarchyData);
            }
            Map<String, Object> updateRequest = new HashMap<String, Object>() {{
                put(JsonKey.REQUEST, new HashMap<String, Object>() {{
                    put(JsonKey.DATA, new HashMap<String, Object>() {{
                        put(JsonKey.NODES_MODIFIED, nodesModified);
                        put(JsonKey.HIERARCHY, hierarchyData);
                    }});
                }});
            }};
            ProjectLogger.log("Create Textbook - UpdateHierarchy Request : " + mapper.writeValueAsString(updateRequest), LoggerEnum.INFO.name());
            return updateHierarchy(tbId, updateRequest);
        }
    }

    private void populateNodes(Map<String, Object> row, String tbId, Map<String, Object> tbMetadata, Map<String, Object> nodesModified, Map<String, Object> hierarchyData) {
        Map<String, Object> hierarchy = (Map<String, Object>) row.get(JsonKey.HIERARCHY);
        hierarchy.remove(StringUtils.capitalize(JsonKey.TEXTBOOK));
        hierarchy.remove(JsonKey.IDENTIFIER);
        String unitType = (String) tbMetadata.get(JsonKey.CONTENT_TYPE) + JsonKey.UNIT;
        String framework = (String) tbMetadata.get(JsonKey.FRAMEWORK);
        int levelCount = 0;
        String code = tbId;
        String parentCode = tbId;
        for (int i = 1; i <= hierarchy.size(); i++) {
            if (StringUtils.isNotBlank((String) hierarchy.get("L:" + i))) {
                String name = (String) hierarchy.get("L:" + i);
                code += name;
                levelCount += 1;
                if (i - 1 > 0)
                    parentCode += (String) hierarchy.get("L:" + (i - 1));
                if(StringUtils.isBlank((String) hierarchy.get("L:" + (i + 1))))
                    populateNodeModified(name, getCode(code), (Map<String, Object>) row.get(JsonKey.METADATA), unitType, framework, nodesModified, true);
                else
                    populateNodeModified(name, getCode(code), null, unitType, framework, nodesModified, true);
                populateHierarchyData(tbId, name, getCode(code), getCode(parentCode), levelCount, hierarchyData);
            } else {
                break;
            }
        }
    }

    private String getCode(String code) {
        return DigestUtils.md5Hex(code);
    }


    private Map<String, Object> getTextbook(String tbId) {
        Map<String, Object> response = TextBookTocUtil.readContent(tbId);
        if (null != response && !response.isEmpty() && StringUtils.equals(OK.name(), (String) response.get(RESPONSE_CODE))) {
            Map<String, Object> result = (Map<String, Object>) response.get(RESULT);
            Map<String, Object> textbook = (Map<String, Object>) result.get(CONTENT);
            return textbook;
        } else {
            ProjectLogger.log("Error while fetching textbook : " + tbId + " with response " + response, LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.errorProcessingRequest.getErrorCode(),
                    ResponseCode.errorProcessingRequest.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        }
    }

    private Response updateTextbook(Request request) throws Exception {
        List<Map<String, Object>> data = (List<Map<String, Object>>) ((Map<String, Object>) request.get(JsonKey.DATA)).get(JsonKey.FILE_DATA);
        if (CollectionUtils.isEmpty(data)) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    ResponseCode.invalidRequestData.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        } else {
            ProjectLogger.log("Update Textbook - UpdateHierarchy input data : " + mapper.writeValueAsString(data), LoggerEnum.INFO.name());
            Map<String, Object> nodesModified = new HashMap<>();
            String tbId = (String) request.get(TEXTBOOK_ID);
            nodesModified.put(tbId, new HashMap<String, Object>(){{
                put(JsonKey.TB_IS_NEW, false);
                put(JsonKey.TB_ROOT, true);
                put(JsonKey.METADATA, new HashMap<String, Object>());
            }});
            for (Map<String, Object> row : data) {
                Map<String, Object> metadata = (Map<String, Object>) row.get(JsonKey.METADATA);
                Map<String, Object> hierarchy = (Map<String, Object>) row.get(JsonKey.HIERARCHY);
                String id = (String) metadata.get(JsonKey.IDENTIFIER);
                metadata.remove(JsonKey.IDENTIFIER);
                populateNodeModified((String) hierarchy.get("L:" + (hierarchy.size()-1)), id, metadata, null, null, nodesModified, false);
            }
            Map<String, Object> updateRequest = new HashMap<String, Object>() {{
                put(JsonKey.REQUEST, new HashMap<String, Object>() {{
                    put(JsonKey.DATA, new HashMap<String, Object>() {{
                        put(JsonKey.NODES_MODIFIED, nodesModified);
                    }});
                }});
            }};
            ProjectLogger.log("Update Textbook - UpdateHierarchy Request : " + mapper.writeValueAsString(updateRequest), LoggerEnum.INFO.name());
            return updateHierarchy((String) request.get(TEXTBOOK_ID), updateRequest);
        }
    }

    private Response updateHierarchy(String tbId, Map<String, Object> updateRequest) throws Exception {
        String requestUrl =
                ProjectUtil.getConfigValue(JsonKey.EKSTEP_BASE_URL)
                        + ProjectUtil.getConfigValue(JsonKey.UPDATE_HIERARCHY_API);
        HttpResponse<String> updateResponse = Unirest.patch(requestUrl).headers(getDefaultHeaders()).body(mapper.writeValueAsString(updateRequest)).asString();
        if (null != updateResponse) {
            Response response = mapper.readValue(updateResponse.getBody(), Response.class);
            if (response.getResponseCode().getResponseCode() == ResponseCode.OK.getResponseCode()) {
                return response;
            } else {
                throw new ProjectCommonException(
                        response.getResponseCode().name(),
                        response.getParams().getErrmsg() + " " + response.getResult(),
                        response.getResponseCode().getResponseCode());
            }
        } else {
            throw new ProjectCommonException(
                    ResponseCode.errorTbUpdate.getErrorCode(),
                    ResponseCode.errorTbUpdate.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        }

    }

    private Map<String, String> getDefaultHeaders() {

        return new HashMap<String, String>() {{
            put("Content-Type", "application/json");
            put(JsonKey.AUTHORIZATION, JsonKey.BEARER + ProjectUtil.getConfigValue(JsonKey.SUNBIRD_AUTHORIZATION));
        }};
    }

    private void download(Request request) {
        Response response = new Response();
        Map<String, Object> textbook = new HashMap<>();
        textbook.put(
                "tocUrl",
                "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1126441512460369921103/artifact/1_1543475510769.pdf");
        textbook.put("ttl", 86400);
        response.getResult().put("textbook", textbook);
        sender().tell(response, sender());
    }

    private void populateNodeModified(String name, String code, Map<String, Object> metadata, String unitType, String framework, Map<String, Object> nodesModified, boolean isNew) {
        Map<String, Object> node = null;
        if (nodesModified.containsKey(code)) {
            node = (Map<String, Object>) nodesModified.get(code);
            if (MapUtils.isNotEmpty(metadata)) {
                Map<String, Object> newMeta = new HashMap<String, Object>() {{
                    List<String> keywords = (StringUtils.isNotBlank((String) metadata.get(JsonKey.KEYWORDS))) ? Arrays.asList(((String) metadata.get(JsonKey.KEYWORDS)).split(",")) : null;
                    List<String> gradeLevel = (StringUtils.isNotBlank((String) metadata.get(JsonKey.GRADE_LEVEL))) ? Arrays.asList(((String) metadata.get(JsonKey.GRADE_LEVEL)).split(",")) : null;
                    putAll(metadata);
                    remove(JsonKey.KEYWORDS);
                    remove(JsonKey.GRADE_LEVEL);
                    if (CollectionUtils.isNotEmpty(keywords))
                        put(JsonKey.KEYWORDS, keywords);
                    if (CollectionUtils.isNotEmpty(gradeLevel))
                        put(JsonKey.GRADE_LEVEL, gradeLevel);
                }};
                ((Map<String, Object>) node.get(JsonKey.METADATA)).putAll(newMeta);
            }
        } else {
            node = new HashMap<String, Object>() {{
                put(JsonKey.TB_IS_NEW, isNew);
                put(JsonKey.TB_ROOT, false);
                put(JsonKey.METADATA, new HashMap<String, Object>() {{
                    if (StringUtils.isNotBlank(name))
                        put(JsonKey.NAME, name);
                    put(JsonKey.MIME_TYPE, JsonKey.COLLECTION_MIME_TYPE);
                    if (StringUtils.isNotBlank(unitType))
                        put(JsonKey.CONTENT_TYPE, unitType);
                    if (StringUtils.isNotBlank(framework))
                        put(JsonKey.FRAMEWORK, framework);
                    if(MapUtils.isNotEmpty(metadata)){
                        List<String> keywords = (StringUtils.isNotBlank((String) metadata.get(JsonKey.KEYWORDS))) ? Arrays.asList(((String) metadata.get(JsonKey.KEYWORDS)).split(",")) : null;
                        List<String> gradeLevel = (StringUtils.isNotBlank((String) metadata.get(JsonKey.GRADE_LEVEL))) ? Arrays.asList(((String) metadata.get(JsonKey.GRADE_LEVEL)).split(",")) : null;
                        putAll(metadata);
                        remove(JsonKey.KEYWORDS);
                        remove(JsonKey.GRADE_LEVEL);
                        if (CollectionUtils.isNotEmpty(keywords))
                            put(JsonKey.KEYWORDS, keywords);
                        if (CollectionUtils.isNotEmpty(gradeLevel))
                            put(JsonKey.GRADE_LEVEL, gradeLevel);
                    }
                }});
            }};
        }
        nodesModified.put(code, node);
    }

    private void populateHierarchyData(String tbId, String name, String code, String parentCode, int levelCount, Map<String, Object> hierarchyData) {
        if (levelCount == 1) {
            parentCode = tbId;
        }
        if (null != hierarchyData.get(code)) {
            ((Map<String, Object>) hierarchyData.get(code)).put(JsonKey.NAME, name);
        } else {
            hierarchyData.put(code, new HashMap<String, Object>() {{
                put(JsonKey.NAME, name);
                put(JsonKey.CHILDREN, new ArrayList<>());
                put(JsonKey.TB_ROOT, false);
            }});
        }

        if (null != hierarchyData.get(parentCode)) {
            List<String> children = ((List) ((Map<String, Object>) hierarchyData.get(parentCode)).get(JsonKey.CHILDREN));
            if (!children.contains(code)) {
                children.add(code);
            }
        } else {
            String finalCode = code;
            hierarchyData.put(parentCode, new HashMap<String, Object>() {{
                put(JsonKey.NAME, "");
                put(JsonKey.CHILDREN, new ArrayList<String>() {{
                    add(finalCode);
                }});
                put(JsonKey.TB_ROOT, false);
            }});
        }
    }
}