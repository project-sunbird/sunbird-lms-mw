package org.sunbird.learner.actors.bulkupload;

import static java.io.File.separator;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.sunbird.common.exception.ProjectCommonException.throwClientErrorException;
import static org.sunbird.common.exception.ProjectCommonException.throwServerErrorException;
import static org.sunbird.common.models.util.JsonKey.CHILDREN;
import static org.sunbird.common.models.util.JsonKey.CONTENT;
import static org.sunbird.common.models.util.JsonKey.CONTENT_TYPE;
import static org.sunbird.common.models.util.JsonKey.DOWNLOAD;
import static org.sunbird.common.models.util.JsonKey.HIERARCHY;
import static org.sunbird.common.models.util.JsonKey.MIME_TYPE;
import static org.sunbird.common.models.util.JsonKey.NAME;
import static org.sunbird.common.models.util.JsonKey.TEXTBOOK;
import static org.sunbird.common.models.util.JsonKey.TEXTBOOK_ID;
import static org.sunbird.common.models.util.JsonKey.TEXTBOOK_TOC_ALLOWED_CONTNET_TYPES;
import static org.sunbird.common.models.util.JsonKey.TEXTBOOK_TOC_ALLOWED_MIMETYPE;
import static org.sunbird.common.models.util.JsonKey.TEXTBOOK_TOC_CSV_TTL;
import static org.sunbird.common.models.util.JsonKey.TOC_URL;
import static org.sunbird.common.models.util.JsonKey.TTL;
import static org.sunbird.common.models.util.JsonKey.VERSION_KEY;
import static org.sunbird.common.models.util.LoggerEnum.ERROR;
import static org.sunbird.common.models.util.LoggerEnum.INFO;
import static org.sunbird.common.models.util.ProjectLogger.log;
import static org.sunbird.common.models.util.ProjectUtil.getConfigValue;
import static org.sunbird.common.models.util.Slug.makeSlug;
import static org.sunbird.common.responsecode.ResponseCode.SERVER_ERROR;
import static org.sunbird.common.responsecode.ResponseCode.invalidTextbook;
import static org.sunbird.common.responsecode.ResponseCode.noChildrenExists;
import static org.sunbird.common.responsecode.ResponseCode.textbookChildrenExist;
import static org.sunbird.content.textbook.FileExtension.Extension.CSV;
import static org.sunbird.content.textbook.TextBookTocUploader.TEXTBOOK_TOC_FOLDER;
import static org.sunbird.content.util.ContentCloudStore.getUri;
import static org.sunbird.content.util.TextBookTocUtil.readContent;
import static org.sunbird.content.util.TextBookTocUtil.readHierarchy;
import static org.sunbird.content.util.TextBookTocUtil.serialize;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TextbookActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.textbook.FileExtension;
import org.sunbird.content.textbook.TextBookTocUploader;
import org.sunbird.content.util.TextBookTocUtil;

@ActorConfig(
  tasks = {"textbookTocUpload", "textbookTocUrl", "textbookTocUpdate"},
  asyncTasks = {}
)
public class TextbookTocActor extends BaseBulkUploadActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    if (request
        .getOperation()
        .equalsIgnoreCase(TextbookActorOperation.TEXTBOOK_TOC_UPLOAD.getValue())) {
      upload(request);
    } else if (request
        .getOperation()
        .equalsIgnoreCase(TextbookActorOperation.TEXTBOOK_TOC_URL.getValue())) {
      getTocUrl(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  @SuppressWarnings("unchecked")
  private void upload(Request request) throws Exception {
    byte[] byteArray = (byte[]) request.getRequest().get(JsonKey.DATA);
    InputStream inputStream = new ByteArrayInputStream(byteArray);
    Map<String, Object> resultMap = readAndValidateCSV(inputStream);
    Set<String> dialCodes = (Set<String>) resultMap.get(JsonKey.DIAL_CODES);
    resultMap.remove(JsonKey.DIAL_CODES);
    Map<String, List<String>> reqDialCodeIdentifierMap =
        (Map<String, List<String>>) resultMap.get(JsonKey.DIAL_CODE_IDENTIFIER_MAP);
    resultMap.remove(JsonKey.DIAL_CODE_IDENTIFIER_MAP);
    Set<String> topics = (Set<String>) resultMap.get(JsonKey.TOPICS);
    resultMap.remove(JsonKey.TOPICS);

    String tbId = (String) request.get(TEXTBOOK_ID);
    Map<String, Object> textbookData = getTextbook(tbId);

    validateTopics(topics, (String) textbookData.get(JsonKey.FRAMEWORK));
    validateDialCodesWithReservedDialCodes(dialCodes, textbookData);
    checkDialCodeUniquenessInTextBookHierarchy(reqDialCodeIdentifierMap, tbId);
    request.getRequest().put(JsonKey.DATA, resultMap);
    String mode = ((Map<String, Object>) request.get(JsonKey.DATA)).get(JsonKey.MODE).toString();
    validateRequest(request, mode, textbookData);
    Response response = new Response();
    if (StringUtils.equalsIgnoreCase(mode, JsonKey.CREATE)) {
      response = createTextbook(request, textbookData);
    } else if (StringUtils.equalsIgnoreCase(mode, JsonKey.UPDATE)) {
      response = updateTextbook(request);
    } else {
      unSupportedMessage();
    }
    sender().tell(response, sender());
  }

  @SuppressWarnings("unchecked")
  private void checkDialCodeUniquenessInTextBookHierarchy(
      Map<String, List<String>> reqDialCodesIdentifierMap, String textbookId) {
    Map<String, Object> contentHierarchy = getHierarchy(textbookId);
    Map<String, List<String>> hierarchyDialCodeIdentifierMap = new HashMap<>();
    List<String> contentDialCodes = (List<String>) contentHierarchy.get(JsonKey.DIAL_CODES);
    if (CollectionUtils.isNotEmpty(contentDialCodes)) {
      hierarchyDialCodeIdentifierMap.put(
          (String) contentHierarchy.get(JsonKey.IDENTIFIER), contentDialCodes);
    }

    List<Map<String, Object>> children =
        (List<Map<String, Object>>) contentHierarchy.get(JsonKey.CHILDREN);
    hierarchyDialCodeIdentifierMap.putAll(getDialCodeIdentifierMap(children));

    if (MapUtils.isNotEmpty(reqDialCodesIdentifierMap)) {
      Map<String, String> reqDialCodeMap =
          convertDialcodeToIdentifierMap(reqDialCodesIdentifierMap);
      if (MapUtils.isNotEmpty(hierarchyDialCodeIdentifierMap)) {
        Map<String, String> hierarchyDialCodeMap =
            convertDialcodeToIdentifierMap(hierarchyDialCodeIdentifierMap);
        validateReqDialCode(reqDialCodeMap, hierarchyDialCodeMap);
      }
    }
  }

  private void validateReqDialCode(
      Map<String, String> reqDialCodeMap, Map<String, String> hierarchyDialCodeMap) {
    reqDialCodeMap.forEach(
        (k, v) -> {
          if (!v.equalsIgnoreCase(hierarchyDialCodeMap.get(k))) {
            throwClientErrorException(
                ResponseCode.errorDialCodeAlreadyAssociated,
                MessageFormat.format(
                    ResponseCode.errorDialCodeAlreadyAssociated.getErrorMessage(),
                    k,
                    hierarchyDialCodeMap.get(k)));
          }
        });
  }

  private Map<String, String> convertDialcodeToIdentifierMap(
      Map<String, List<String>> identifierDialCodeMap) {
    Map<String, String> dialCodeIdentifierMap = new HashMap<>();
    if (MapUtils.isNotEmpty(identifierDialCodeMap)) {
      identifierDialCodeMap.forEach(
          (k, v) -> {
            v.forEach(
                dialcode -> {
                  if (dialCodeIdentifierMap.containsKey(dialcode)) {
                    throwClientErrorException(
                        ResponseCode.errorDialCodeDuplicateEntry,
                        MessageFormat.format(
                            ResponseCode.errorDialCodeDuplicateEntry.getErrorMessage(), k, v));
                  } else {
                    dialCodeIdentifierMap.put(dialcode, k);
                  }
                });
          });
    }
    return dialCodeIdentifierMap;
  }

  @SuppressWarnings("unchecked")
  public Map<String, List<String>> getDialCodeIdentifierMap(List<Map<String, Object>> children) {
    Map<String, List<String>> hierarchyDialCodeIdentifierMap = new HashMap<>();
    for (Map<String, Object> child : children) {
      if (CollectionUtils.isNotEmpty((List<String>) child.get(JsonKey.DIAL_CODES))) {
        hierarchyDialCodeIdentifierMap.put(
            (String) child.get(JsonKey.IDENTIFIER), (List<String>) child.get(JsonKey.DIAL_CODES));
      }
      if (CollectionUtils.isNotEmpty((List<Map<String, Object>>) child.get(JsonKey.CHILDREN))) {
        getDialCodeIdentifierMap((List<Map<String, Object>>) child.get(JsonKey.CHILDREN));
      }
    }
    return hierarchyDialCodeIdentifierMap;
  }

  private void validateTopics(Set<String> topics, String frameworkId) {
    if (CollectionUtils.isNotEmpty(topics)) {
      List<String> frameworkTopics = getRelatedFrameworkTopics(frameworkId);
      Set<String> invalidTopics = new HashSet<>();
      topics.forEach(
          name -> {
            if (!frameworkTopics.contains(name)) {
              invalidTopics.add(name);
            }
          });
      if (CollectionUtils.isNotEmpty(invalidTopics)) {
        throwClientErrorException(
            ResponseCode.errorInvalidTopic,
            MessageFormat.format(
                ResponseCode.errorInvalidTopic.getErrorMessage(),
                StringUtils.join(invalidTopics, ',')));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private List<String> getRelatedFrameworkTopics(String frameworkId) {
    Response response = TextBookTocUtil.getRelatedFrameworkById(frameworkId);
    Map<String, Object> result = response.getResult();
    List<Map<String, Object>> terms = new ArrayList<>();
    if (MapUtils.isNotEmpty(result)) {
      Map<String, Object> framework = (Map<String, Object>) result.get(JsonKey.FRAMEWORK);
      if (MapUtils.isNotEmpty(framework)) {
        List<Map<String, Object>> categories =
            (List<Map<String, Object>>) framework.get(JsonKey.CATEGORIES);
        if (CollectionUtils.isNotEmpty(categories))
          categories.forEach(
              s -> {
                if (JsonKey.TOPIC.equalsIgnoreCase((String) s.get(JsonKey.CODE))) {
                  terms.addAll((List<Map<String, Object>>) s.get(JsonKey.TERMS));
                }
              });
      }
    }
    return getTopic(terms);
  }

  @SuppressWarnings("unchecked")
  public List<String> getTopic(List<Map<String, Object>> children) {
    List<String> topics = new ArrayList<>();
    for (Map<String, Object> child : children) {
      topics.add((String) child.get(JsonKey.NAME));
      if (null != child.get(JsonKey.CHILDREN)) {
        getTopic((List<Map<String, Object>>) child.get(JsonKey.CHILDREN));
      }
    }
    return topics;
  }

  @SuppressWarnings("unchecked")
  private void validateDialCodesWithReservedDialCodes(
      Set<String> dialCodes, Map<String, Object> textBookdata) {
    List<String> reservedDialCodes = (List<String>) textBookdata.get(JsonKey.RESERVED_DIAL_CODES);
    Set<String> invalidDialCodes = new HashSet<>();
    if (CollectionUtils.isNotEmpty(reservedDialCodes)) {
      dialCodes.forEach(
          dialCode -> {
            if (!reservedDialCodes.contains(dialCode)) {
              invalidDialCodes.add(dialCode);
            }
          });
      if (CollectionUtils.isNotEmpty(invalidDialCodes)) {
        throwDialCodeNotReservedError(textBookdata, invalidDialCodes);
      }
    } else if (CollectionUtils.isNotEmpty(dialCodes)) {
      throwDialCodeNotReservedError(textBookdata, dialCodes);
    }
  }

  private void throwDialCodeNotReservedError(
      Map<String, Object> textBookdata, Set<String> invalidDialCodes) {
    throwClientErrorException(
        ResponseCode.errorDialCodeNotReservedForTextBook,
        MessageFormat.format(
            ResponseCode.errorDialCodeNotReservedForTextBook.getErrorMessage(),
            StringUtils.join(invalidDialCodes, ','),
            textBookdata.get(JsonKey.IDENTIFIER)));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> readAndValidateCSV(InputStream inputStream) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> result = new HashMap<>();
    List<Map<String, Object>> rows = new ArrayList<>();

    String tocMapping = ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_INPUT_MAPPING);
    Map<String, Object> configMap =
        mapper.readValue(tocMapping, new TypeReference<Map<String, Object>>() {});

    Map<String, String> metadata = (Map<String, String>) configMap.get(JsonKey.METADATA);
    Map<String, String> hierarchy = (Map<String, String>) configMap.get(JsonKey.HIERARCHY);
    Map<String, String> fwMetadata =
        (Map<String, String>) configMap.get(JsonKey.FRAMEWORK_METADATA);
    String id =
        configMap
            .getOrDefault(JsonKey.IDENTIFIER, StringUtils.capitalize(JsonKey.IDENTIFIER))
            .toString();
    metadata.putAll(fwMetadata);

    CSVParser csvFileParser = null;

    CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader();

    try (InputStreamReader reader = new InputStreamReader(inputStream, "UTF8"); ) {
      csvFileParser = csvFileFormat.parse(reader);
      Map<String, Integer> csvHeaders = csvFileParser.getHeaderMap();

      String mode = csvHeaders.containsKey(id) ? JsonKey.UPDATE : JsonKey.CREATE;
      result.put(JsonKey.MODE, mode);

      if (null != csvHeaders && !csvHeaders.isEmpty()) {
        metadata.values().removeIf(key -> !csvHeaders.keySet().contains(key));
        hierarchy.values().removeIf(key -> !csvHeaders.keySet().contains(key));
      } else {
        throwClientErrorException(ResponseCode.blankCsvData);
      }
      List<CSVRecord> csvRecords = csvFileParser.getRecords();
      validateCSV(csvRecords);
      Set<String> dialCodes = new HashSet<>();
      Map<String, List<String>> dialCodeIdentifierMap = new HashMap<>();
      Set<String> topics = new HashSet<>();
      for (int i = 0; i < csvRecords.size(); i++) {
        CSVRecord record = csvRecords.get(i);

        HashMap<String, Object> recordMap = new HashMap<>();
        HashMap<String, Object> hierarchyMap = new HashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
          if (StringUtils.isNotBlank(record.get(entry.getValue())))
            recordMap.put(entry.getKey(), record.get(entry.getValue()));
        }
        for (Map.Entry<String, String> entry : hierarchy.entrySet()) {
          if (StringUtils.isNotBlank(record.get(entry.getValue())))
            hierarchyMap.put(entry.getKey(), record.get(entry.getValue()));
        }

        if (!(MapUtils.isEmpty(recordMap) && MapUtils.isEmpty(hierarchyMap))) {
          validateQrCodeRequiredAndQrCode(recordMap);
          String dialCode = (String) recordMap.get(JsonKey.DIAL_CODES);
          List<String> dialCodeList = null;
          if (StringUtils.isNotBlank(dialCode)) {
            dialCodeList = new ArrayList<String>(Arrays.asList(dialCode.split(",")));
            for (String dCode : dialCodeList) {
              if (!dialCodes.add(dCode.trim())) {
                throwClientErrorException(
                    ResponseCode.errorDuplicateEntries,
                    MessageFormat.format(
                        ResponseCode.errorDuplicateEntries.getErrorMessage(), dialCode));
              }
            }
          }

          String reqTopics = (String) recordMap.get(JsonKey.TOPIC);
          if (StringUtils.isNotBlank(reqTopics)) {
            List<String> topicList = new ArrayList<String>(Arrays.asList(reqTopics.split(",")));
            topicList.forEach(
                s -> {
                  topics.add(s.trim());
                });
          }

          Map<String, Object> map = new HashMap<>();
          if (JsonKey.UPDATE.equalsIgnoreCase(mode) && StringUtils.isNotBlank(record.get(id))) {
            String identifier = record.get(id).trim();
            map.put(JsonKey.IDENTIFIER, identifier);
            if (CollectionUtils.isNotEmpty(dialCodeList)) {
              dialCodeIdentifierMap.put(identifier, dialCodeList);
            }
          }
          map.put(JsonKey.METADATA, recordMap);
          map.put(JsonKey.HIERARCHY, hierarchyMap);
          rows.add(map);
        }
      }

      result.put(JsonKey.FILE_DATA, rows);
      result.put(JsonKey.DIAL_CODES, dialCodes);
      result.put(JsonKey.TOPICS, topics);
      result.put(JsonKey.DIAL_CODE_IDENTIFIER_MAP, dialCodeIdentifierMap);
    } catch (ProjectCommonException e) {
      throw e;
    } catch (Exception e) {
      throwServerErrorException(ResponseCode.errorProcessingFile);
    } finally {
      try {
        if (null != csvFileParser) csvFileParser.close();
      } catch (IOException e) {
        ProjectLogger.log(
            "TextbookTocActor:readAndValidateCSV : Exception occurred while closing stream");
      }
    }
    return result;
  }

  private void validateQrCodeRequiredAndQrCode(Map<String, Object> recordMap) {
    if (JsonKey.NO.equalsIgnoreCase((String) recordMap.get(JsonKey.DIAL_CODE_REQUIRED))
        && StringUtils.isNotBlank((String) recordMap.get(JsonKey.DIAL_CODES))) {
      String errorMessage =
          MessageFormat.format(
              ResponseCode.errorConflictingValues.getErrorMessage(),
              JsonKey.QR_CODE_REQUIRED,
              JsonKey.NO,
              JsonKey.QR_CODE,
              recordMap.get(JsonKey.DIAL_CODES));
      throwClientErrorException(ResponseCode.errorConflictingValues, errorMessage);
    }
  }

  private void validateCSV(List<CSVRecord> records) {
    if (CollectionUtils.isEmpty(records)) {
      throwClientErrorException(
          ResponseCode.blankCsvData, ResponseCode.blankCsvData.getErrorMessage());
    }
    Integer allowedNumberOfRecord =
        Integer.valueOf(ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_MAX_CSV_ROWS));
    if (CollectionUtils.isNotEmpty(records) && records.size() > allowedNumberOfRecord) {
      throwClientErrorException(
          ResponseCode.csvRowsExceeds,
          ResponseCode.csvRowsExceeds.getErrorMessage() + allowedNumberOfRecord);
    }
  }

  private void getTocUrl(Request request) {
    String textbookId = (String) request.get(TEXTBOOK_ID);
    if (isBlank(textbookId)) {
      log("Invalid TextBook Provided", ERROR.name());
      throwClientErrorException(invalidTextbook, invalidTextbook.getErrorMessage());
    }
    log("Reading Content for TextBook | Id: " + textbookId, INFO.name());
    Map<String, Object> content = getTextbook(textbookId);
    validateTextBook(content, DOWNLOAD);
    FileExtension fileExtension = CSV.getFileExtension();
    String contentVersionKey = (String) content.get(VERSION_KEY);
    String textBookNameSlug = makeSlug((String) content.get(NAME), true);
    String textBookTocFileName = textbookId + "_" + textBookNameSlug + "_" + contentVersionKey;
    String prefix =
        TEXTBOOK_TOC_FOLDER + separator + textBookTocFileName + fileExtension.getDotExtension();
    log("Fetching TextBook Toc URL from Cloud", INFO.name());

    String cloudPath = getUri(prefix, false);
    if (isBlank(cloudPath)) {
      log("Reading Hierarchy for TextBook | Id: " + textbookId, INFO.name());
      Map<String, Object> contentHierarchy = getHierarchy(textbookId);
      String hierarchyVersionKey = (String) contentHierarchy.get(VERSION_KEY);
      cloudPath =
          new TextBookTocUploader(textBookTocFileName, fileExtension)
              .execute(contentHierarchy, textbookId, hierarchyVersionKey);
    }

    log("Sending Response for Toc Download API for TextBook | Id: " + textbookId, INFO.name());
    Map<String, Object> textbook = new HashMap<>();
    textbook.put(TOC_URL, cloudPath);
    textbook.put(TTL, getConfigValue(TEXTBOOK_TOC_CSV_TTL));
    Response response = new Response();
    response.put(TEXTBOOK, textbook);

    sender().tell(response, sender());
  }

  @SuppressWarnings("unchecked")
  private void validateTextBook(Map<String, Object> textbook, String mode) {
    List<String> allowedContentTypes =
        asList(getConfigValue(TEXTBOOK_TOC_ALLOWED_CONTNET_TYPES).split(","));
    if (!TEXTBOOK_TOC_ALLOWED_MIMETYPE.equalsIgnoreCase(textbook.get(MIME_TYPE).toString())
        || !allowedContentTypes.contains(textbook.get(CONTENT_TYPE).toString())) {
      throwClientErrorException(invalidTextbook, invalidTextbook.getErrorMessage());
    }
    List<Object> children = (List<Object>) textbook.get(CHILDREN);
    if (JsonKey.CREATE.equalsIgnoreCase(mode)) {
      if (null != children && !children.isEmpty()) {
        throwClientErrorException(textbookChildrenExist, textbookChildrenExist.getErrorMessage());
      }
    } else if (DOWNLOAD.equalsIgnoreCase(mode)) {
      if (null == children || children.isEmpty())
        throwClientErrorException(noChildrenExists, noChildrenExists.getErrorMessage());
    }
  }

  @SuppressWarnings("unchecked")
  private void validateRequest(Request request, String mode, Map<String, Object> textbook)
      throws IOException {
    Set<String> rowsHash = new HashSet<>();
    String mandatoryFields = getConfigValue(JsonKey.TEXTBOOK_TOC_MANDATORY_FIELDS);
    Map<String, String> mandatoryFieldsMap =
        mapper.readValue(mandatoryFields, new TypeReference<Map<String, String>>() {});
    String textbookName = (String) textbook.get(JsonKey.NAME);

    validateTextBook(textbook, mode);

    List<Map<String, Object>> fileData =
        (List<Map<String, Object>>)
            ((Map<String, Object>) request.get(JsonKey.DATA)).get(JsonKey.FILE_DATA);

    for (int i = 0; i < fileData.size(); i++) {
      Map<String, Object> row = fileData.get(i);
      Boolean isAdded =
          rowsHash.add(
              DigestUtils.md5Hex(SerializationUtils.serialize(row.get(HIERARCHY).toString())));
      if (!isAdded) {
        throwClientErrorException(
            ResponseCode.duplicateRows, ResponseCode.duplicateRows.getErrorMessage() + (i + 1));
      }
      Map<String, Object> hierarchy = (Map<String, Object>) row.get(JsonKey.HIERARCHY);

      String name = (String) hierarchy.getOrDefault(StringUtils.capitalize(JsonKey.TEXTBOOK), "");
      if (isBlank(name) || !StringUtils.equalsIgnoreCase(name, textbookName)) {
        log(
            "Name mismatch. Content has: " + name + " but, file has: " + textbookName,
            null,
            ERROR.name());
        throwClientErrorException(
            ResponseCode.invalidTextbookName, ResponseCode.invalidTextbookName.getErrorMessage());
      }
      for (String field : mandatoryFieldsMap.keySet()) {
        if (!hierarchy.containsKey(field)
            || isBlank(hierarchy.getOrDefault(field, "").toString())) {
          throwClientErrorException(
              ResponseCode.requiredFieldMissing,
              ResponseCode.requiredFieldMissing.getErrorMessage() + mandatoryFieldsMap.values());
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Response createTextbook(Request request, Map<String, Object> textBookdata)
      throws Exception {
    log("Create Textbook called ", INFO.name());
    Map<String, Object> file = (Map<String, Object>) request.get(JsonKey.DATA);
    List<Map<String, Object>> data = (List<Map<String, Object>>) file.get(JsonKey.FILE_DATA);
    log("Create Textbook - UpdateHierarchy input data : " + mapper.writeValueAsString(data));
    if (CollectionUtils.isEmpty(data)) {
      throw new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    } else {
      String tbId = (String) request.get(TEXTBOOK_ID);
      Map<String, Object> nodesModified = new HashMap<>();
      Map<String, Object> hierarchyData = new HashMap<>();
      nodesModified.put(
          tbId,
          new HashMap<String, Object>() {
            {
              put(JsonKey.TB_IS_NEW, false);
              put(JsonKey.TB_ROOT, true);
              put(JsonKey.METADATA, new HashMap<String, Object>());
            }
          });

      hierarchyData.put(
          tbId,
          new HashMap<String, Object>() {
            {
              put(JsonKey.NAME, textBookdata.get(JsonKey.NAME));
              put(CONTENT_TYPE, textBookdata.get(CONTENT_TYPE));
              put(CHILDREN, new ArrayList<>());
              put(JsonKey.TB_ROOT, true);
            }
          });
      for (Map<String, Object> row : data) {
        populateNodes(row, tbId, textBookdata, nodesModified, hierarchyData);
      }
      Map<String, Object> updateRequest =
          new HashMap<String, Object>() {
            {
              put(
                  JsonKey.REQUEST,
                  new HashMap<String, Object>() {
                    {
                      put(
                          JsonKey.DATA,
                          new HashMap<String, Object>() {
                            {
                              put(JsonKey.NODES_MODIFIED, nodesModified);
                              put(JsonKey.HIERARCHY, hierarchyData);
                            }
                          });
                    }
                  });
            }
          };
      log(
          "Create Textbook - UpdateHierarchy Request : " + mapper.writeValueAsString(updateRequest),
          INFO.name());
      return updateHierarchy(tbId, updateRequest);
    }
  }

  private void populateNodes(
      Map<String, Object> row,
      String tbId,
      Map<String, Object> tbMetadata,
      Map<String, Object> nodesModified,
      Map<String, Object> hierarchyData) {
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
        if (i - 1 > 0) parentCode += (String) hierarchy.get("L:" + (i - 1));
        if (isBlank((String) hierarchy.get("L:" + (i + 1))))
          populateNodeModified(
              name,
              getCode(code),
              (Map<String, Object>) row.get(JsonKey.METADATA),
              unitType,
              framework,
              nodesModified,
              true);
        else
          populateNodeModified(name, getCode(code), null, unitType, framework, nodesModified, true);
        populateHierarchyData(
            tbId, name, getCode(code), getCode(parentCode), levelCount, hierarchyData);
      } else {
        break;
      }
    }
  }

  private String getCode(String code) {
    return DigestUtils.md5Hex(code);
  }

  private Map<String, Object> getTextbook(String tbId) {
    Response response = null;
    Map<String, Object> textbook;
    try {
      response = readContent(tbId);
      textbook = (Map<String, Object>) response.get(CONTENT);
      if (null == textbook) {
        log("Empty Content fetched | TextBook Id: " + tbId);
        throwServerErrorException(SERVER_ERROR, "Empty Content fetched for TextBook Id: " + tbId);
      }
    } catch (Exception e) {
      log(
          "Error while fetching textbook : " + tbId + " with response " + serialize(response),
          ERROR.name());
      throw e;
    }
    return textbook;
  }

  private Map<String, Object> getHierarchy(String tbId) {
    Response response = null;
    Map<String, Object> hierarchy;
    try {
      response = readHierarchy(tbId);
      hierarchy = (Map<String, Object>) response.get(CONTENT);
      if (null == hierarchy) {
        log("Empty Hierarchy fetched | TextBook Id: " + tbId);
        throwServerErrorException(SERVER_ERROR, "Empty Hierarchy fetched for TextBook Id: " + tbId);
      }
    } catch (Exception e) {
      log(
          "Error while fetching textbook : " + tbId + " with response " + serialize(response),
          ERROR.name());
      throw e;
    }
    return hierarchy;
  }

  @SuppressWarnings("unchecked")
  private Response updateTextbook(Request request) throws Exception {
    List<Map<String, Object>> data =
        (List<Map<String, Object>>)
            ((Map<String, Object>) request.get(JsonKey.DATA)).get(JsonKey.FILE_DATA);
    if (CollectionUtils.isEmpty(data)) {
      throw new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    } else {
      log(
          "Update Textbook - UpdateHierarchy input data : " + mapper.writeValueAsString(data),
          INFO.name());
      Map<String, Object> nodesModified = new HashMap<>();
      String tbId = (String) request.get(TEXTBOOK_ID);
      nodesModified.put(
          tbId,
          new HashMap<String, Object>() {
            {
              put(JsonKey.TB_IS_NEW, false);
              put(JsonKey.TB_ROOT, true);
              put(JsonKey.METADATA, new HashMap<String, Object>());
            }
          });
      for (Map<String, Object> row : data) {
        Map<String, Object> metadata = (Map<String, Object>) row.get(JsonKey.METADATA);
        Map<String, Object> hierarchy = (Map<String, Object>) row.get(JsonKey.HIERARCHY);
        String id = (String) row.get(JsonKey.IDENTIFIER);
        metadata.remove(JsonKey.IDENTIFIER);
        populateNodeModified(
            (String) hierarchy.get("L:" + (hierarchy.size() - 1)),
            id,
            metadata,
            null,
            null,
            nodesModified,
            false);
      }
      Map<String, Object> updateRequest =
          new HashMap<String, Object>() {
            {
              put(
                  JsonKey.REQUEST,
                  new HashMap<String, Object>() {
                    {
                      put(
                          JsonKey.DATA,
                          new HashMap<String, Object>() {
                            {
                              put(JsonKey.NODES_MODIFIED, nodesModified);
                            }
                          });
                    }
                  });
            }
          };
      log(
          "Update Textbook - UpdateHierarchy Request : " + mapper.writeValueAsString(updateRequest),
          INFO.name());
      return updateHierarchy((String) request.get(TEXTBOOK_ID), updateRequest);
    }
  }

  private Response updateHierarchy(String tbId, Map<String, Object> updateRequest)
      throws Exception {

    String requestUrl =
        getConfigValue(JsonKey.EKSTEP_BASE_URL) + getConfigValue(JsonKey.UPDATE_HIERARCHY_API);
    HttpResponse<String> updateResponse =
        Unirest.patch(requestUrl)
            .headers(getDefaultHeaders())
            .body(mapper.writeValueAsString(updateRequest))
            .asString();
    if (null != updateResponse) {
      Response response = mapper.readValue(updateResponse.getBody(), Response.class);
      if (response.getResponseCode().getResponseCode() == ResponseCode.OK.getResponseCode()) {
        return response;
      } else {
        Map<String, Object> resultMap =
            Optional.ofNullable(response.getResult()).orElse(new HashMap<>());
        String message = "Textbook hierarchy could not be created or updated. ";
        if (MapUtils.isNotEmpty(resultMap)) {
          Object obj = Optional.ofNullable(resultMap.get(JsonKey.TB_MESSAGES)).orElse("");
          if (obj instanceof List) {
            message += ((List<String>) obj).stream().collect(Collectors.joining(";"));
          } else {
            message += String.valueOf(obj);
          }
        }
        throw new ProjectCommonException(
            response.getResponseCode().name(),
            message,
            response.getResponseCode().getResponseCode());
      }
    } else {
      throw new ProjectCommonException(
          ResponseCode.errorTbUpdate.getErrorCode(),
          ResponseCode.errorTbUpdate.getErrorMessage(),
          SERVER_ERROR.getResponseCode());
    }
  }

  private Map<String, String> getDefaultHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    headers.put(
        JsonKey.AUTHORIZATION, JsonKey.BEARER + getConfigValue(JsonKey.SUNBIRD_AUTHORIZATION));
    return headers;
  }

  @SuppressWarnings("unchecked")
  private void populateNodeModified(
      String name,
      String code,
      Map<String, Object> metadata,
      String unitType,
      String framework,
      Map<String, Object> nodesModified,
      boolean isNew) {
    Map<String, Object> node = null;
    if (nodesModified.containsKey(code)) {
      node = (Map<String, Object>) nodesModified.get(code);
      if (MapUtils.isNotEmpty(metadata)) {
        Map<String, Object> newMeta = initializeMetaDataForModifiedNode(metadata);
        ((Map<String, Object>) node.get(JsonKey.METADATA)).putAll(newMeta);
      }
    } else {
      Map<String, Object> newMeta = initializeMetaDataForModifiedNode(metadata);
      node = new HashMap<String, Object>();
      node.put(JsonKey.TB_IS_NEW, isNew);
      node.put(JsonKey.TB_ROOT, false);
      if (StringUtils.isNotBlank(name)) newMeta.put(JsonKey.NAME, name);
      newMeta.put(JsonKey.MIME_TYPE, JsonKey.COLLECTION_MIME_TYPE);
      if (StringUtils.isNotBlank(unitType)) newMeta.put(JsonKey.CONTENT_TYPE, unitType);
      if (StringUtils.isNotBlank(framework)) newMeta.put(JsonKey.FRAMEWORK, framework);
      node.put(JsonKey.METADATA, newMeta);
    }
    nodesModified.put(code, node);
  }

  private Map<String, Object> initializeMetaDataForModifiedNode(Map<String, Object> metadata) {
    Map<String, Object> newMeta = new HashMap<String, Object>();
    List<String> keywords =
        (StringUtils.isNotBlank((String) metadata.get(JsonKey.KEYWORDS)))
            ? asList(((String) metadata.get(JsonKey.KEYWORDS)).split(","))
            : null;
    List<String> gradeLevel =
        (StringUtils.isNotBlank((String) metadata.get(JsonKey.GRADE_LEVEL)))
            ? asList(((String) metadata.get(JsonKey.GRADE_LEVEL)).split(","))
            : null;
    List<String> dialCodes =
        (StringUtils.isNotBlank((String) metadata.get(JsonKey.DIAL_CODES)))
            ? asList(((String) metadata.get(JsonKey.DIAL_CODES)).split(","))
            : null;

    List<String> topics =
        (StringUtils.isNotBlank((String) metadata.get(JsonKey.TOPIC)))
            ? asList(((String) metadata.get(JsonKey.TOPIC)).split(","))
            : null;
    newMeta.putAll(metadata);
    newMeta.remove(JsonKey.KEYWORDS);
    newMeta.remove(JsonKey.GRADE_LEVEL);
    newMeta.remove(JsonKey.DIAL_CODES);
    newMeta.remove(JsonKey.TOPIC);
    if (CollectionUtils.isNotEmpty(keywords)) newMeta.put(JsonKey.KEYWORDS, keywords);
    if (CollectionUtils.isNotEmpty(gradeLevel)) newMeta.put(JsonKey.GRADE_LEVEL, gradeLevel);
    if (CollectionUtils.isNotEmpty(dialCodes)) {
      List<String> dCodes = new ArrayList<>();
      dialCodes.forEach(
          s -> {
            dCodes.add(s.trim());
          });
      newMeta.put(JsonKey.DIAL_CODES, dCodes);
    }
    if (CollectionUtils.isNotEmpty(topics)) {
      List<String> topicList = new ArrayList<>();
      topics.forEach(
          s -> {
            topicList.add(s.trim());
          });
      newMeta.put(JsonKey.TOPIC, topicList);
    }
    return newMeta;
  }

  private void populateHierarchyData(
      String tbId,
      String name,
      String code,
      String parentCode,
      int levelCount,
      Map<String, Object> hierarchyData) {
    if (levelCount == 1) {
      parentCode = tbId;
    }
    if (null != hierarchyData.get(code)) {
      ((Map<String, Object>) hierarchyData.get(code)).put(JsonKey.NAME, name);
    } else {
      hierarchyData.put(
          code,
          new HashMap<String, Object>() {
            {
              put(JsonKey.NAME, name);
              put(CHILDREN, new ArrayList<>());
              put(JsonKey.TB_ROOT, false);
            }
          });
    }

    if (null != hierarchyData.get(parentCode)) {
      List<String> children =
          ((List) ((Map<String, Object>) hierarchyData.get(parentCode)).get(CHILDREN));
      if (!children.contains(code)) {
        children.add(code);
      }
    } else {
      String finalCode = code;
      hierarchyData.put(
          parentCode,
          new HashMap<String, Object>() {
            {
              put(JsonKey.NAME, "");
              put(
                  CHILDREN,
                  new ArrayList<String>() {
                    {
                      add(finalCode);
                    }
                  });
              put(JsonKey.TB_ROOT, false);
            }
          });
    }
  }
}
