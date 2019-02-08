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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.sunbird.content.textbook.FileExtension;
import org.sunbird.content.textbook.TextBookTocUploader;
import org.sunbird.content.util.TextBookTocUtil;

@ActorConfig(
  tasks = {"textbookTocUpload", "textbookTocUrl", "textbookTocUpdate"},
  asyncTasks = {}
)
public class TextbookTocActor extends BaseBulkUploadActor {

  final String TEXTBOOK_NAME = "Textbook Name";
  final String L1_TEXTBOOK_UNIT = "Level 1 Textbook Unit";

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
    Map<Integer, List<String>> rowNumVsContentIdsMap =
        (Map<Integer, List<String>>) resultMap.get(JsonKey.LINKED_CONTENT);
    resultMap.remove(JsonKey.LINKED_CONTENT);
    validateLinkedContents(rowNumVsContentIdsMap);
    resultMap.put(JsonKey.LINKED_CONTENT, false);
    for (Entry<Integer, List<String>> entry : rowNumVsContentIdsMap.entrySet()) {
      if (CollectionUtils.isNotEmpty(entry.getValue())) {
        resultMap.put(JsonKey.LINKED_CONTENT, true);
        break;
      }
    }
    String tbId = (String) request.get(TEXTBOOK_ID);
    Map<String, Object> textbookData = getTextbook(tbId);
    Map<String, Object> hierarchy = getHierarchy(tbId);
    validateTopics(topics, (String) textbookData.get(JsonKey.FRAMEWORK));
    validateDialCodesWithReservedDialCodes(dialCodes, textbookData);
    checkDialCodeUniquenessInTextBookHierarchy(reqDialCodeIdentifierMap, hierarchy);
    request.getRequest().put(JsonKey.DATA, resultMap);
    String mode = ((Map<String, Object>) request.get(JsonKey.DATA)).get(JsonKey.MODE).toString();
    validateRequest(request, mode, textbookData);
    Response response = new Response();
    if (StringUtils.equalsIgnoreCase(mode, JsonKey.CREATE)) {
      response = createTextbook(request, textbookData);
    } else if (StringUtils.equalsIgnoreCase(mode, JsonKey.UPDATE)) {
      response = updateTextbook(request, textbookData, hierarchy);
    } else {
      unSupportedMessage();
    }
    sender().tell(response, sender());
  }

  private void validateLinkedContents(Map<Integer, List<String>> rowNumVsContentIdsMap) {
    // rowNumVsContentIdsMap convert to contentIdVsrowListMap
    if (MapUtils.isNotEmpty(rowNumVsContentIdsMap)) {
      Map<String, List<Integer>> contentIdVsRowNumMap = new HashMap<>();
      rowNumVsContentIdsMap.forEach(
          (k, v) -> {
            v.forEach(
                contentId -> {
                  if (contentIdVsRowNumMap.containsKey(contentId)) {
                    contentIdVsRowNumMap.get(contentId).add(k);
                  } else {
                    List<Integer> rowNumList = new ArrayList<>();
                    rowNumList.add(k);
                    contentIdVsRowNumMap.put(contentId, rowNumList);
                  }
                });
          });
      callSearchApiForContentIdsValidation(contentIdVsRowNumMap);
    }
  }

  private void callSearchApiForContentIdsValidation(
      Map<String, List<Integer>> contentIdVsRowNumMap) {
    List<String> contentIds = new ArrayList<>();
    contentIds.addAll(contentIdVsRowNumMap.keySet());
    Map<String, Object> requestMap = new HashMap<>();
    Map<String, Object> request = new HashMap<>();
    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.STATUS, "Live");
    filters.put(JsonKey.IDENTIFIER, contentIds);
    request.put(JsonKey.FILTERS, filters);
    requestMap.put(JsonKey.REQUEST, request);
    List<String> fields = new ArrayList<>();
    fields.add(JsonKey.IDENTIFIER);
    request.put(JsonKey.FIELDS, fields);
    if (CollectionUtils.isNotEmpty(contentIds)) {
      String requestUrl =
          getConfigValue(JsonKey.EKSTEP_BASE_URL) + getConfigValue(JsonKey.SUNBIRD_CS_SEARCH_PATH);
      HttpResponse<String> updateResponse = null;
      try {
        updateResponse =
            Unirest.post(requestUrl)
                .headers(getDefaultHeaders())
                .body(mapper.writeValueAsString(requestMap))
                .asString();
        if (null != updateResponse) {
          Response response = mapper.readValue(updateResponse.getBody(), Response.class);
          if (response.getResponseCode().getResponseCode() == ResponseCode.OK.getResponseCode()) {
            Map<String, Object> result = response.getResult();
            List<String> searchedContentIds = new ArrayList<>();
            if (MapUtils.isNotEmpty(result)) {
              int count = (int) result.get(JsonKey.COUNT);
              if (0 == count) {
                Map<String, String> errorMap =
                    prepareErrorMap(contentIdVsRowNumMap, searchedContentIds);
                ProjectCommonException.throwClientErrorException(
                    ResponseCode.errorInvalidLinkedContentUrl,
                    mapper.convertValue(errorMap, String.class));
              }
              List<Map<String, Object>> content =
                  (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
              if (CollectionUtils.isNotEmpty(content)) {
                content.forEach(
                    contentMap -> {
                      searchedContentIds.add((String) contentMap.get(JsonKey.IDENTIFIER));
                    });
                if (content.size() != contentIds.size()) {
                  Map<String, String> errorMap =
                      prepareErrorMap(contentIdVsRowNumMap, searchedContentIds);
                  if (errorMap.size() == 1) {
                    for (Map.Entry<String, String> entry : errorMap.entrySet()) {
                      throwInvalidLinkedContentUrl(
                          entry.getValue(), contentIdVsRowNumMap.get(entry.getKey()));
                    }
                  } else {
                    ProjectCommonException.throwClientErrorException(
                        ResponseCode.errorInvalidLinkedContentUrl,
                        mapper.convertValue(errorMap, String.class));
                  }
                }
              } else {
                throwCompositeSearchFailureError();
              }
            }
          } else {
            throwCompositeSearchFailureError();
          }
        } else {
          throwCompositeSearchFailureError();
        }
      } catch (Exception e) {
        ProjectLogger.log(
            "TextbookTocActor:validateLinkedContents : Error occurred with message "
                + e.getMessage(),
            e);
        throwCompositeSearchFailureError();
      }
    }
  }

  private Map<String, String> prepareErrorMap(
      Map<String, List<Integer>> contentIdVsRowNumMap, List<String> searchedContentIds) {
    Map<String, String> errorMap = new HashMap<>();
    contentIdVsRowNumMap
        .keySet()
        .forEach(
            contentId -> {
              if (!searchedContentIds.contains(contentId)) {
                String contentUrl =
                    ProjectUtil.getConfigValue(JsonKey.SUNBIRD_LINKED_CONTENT_BASE_URL) + contentId;
                String message =
                    MessageFormat.format(
                        ResponseCode.errorInvalidLinkedContentUrl.getErrorMessage(),
                        contentUrl,
                        contentIdVsRowNumMap.get(contentId));
                errorMap.put(contentId, message);
              }
            });
    return errorMap;
  }

  private void throwCompositeSearchFailureError() {
    ProjectCommonException.throwServerErrorException(
        ResponseCode.customServerError, "Exception occurred while validating linked content.");
  }

  @SuppressWarnings("unchecked")
  private void checkDialCodeUniquenessInTextBookHierarchy(
      Map<String, List<String>> reqDialCodesIdentifierMap, Map<String, Object> contentHierarchy) {
    if (MapUtils.isNotEmpty(reqDialCodesIdentifierMap)) {
      Map<String, List<String>> hierarchyDialCodeIdentifierMap = new HashMap<>();
      List<String> contentDialCodes = (List<String>) contentHierarchy.get(JsonKey.DIAL_CODES);
      if (CollectionUtils.isNotEmpty(contentDialCodes)) {
        hierarchyDialCodeIdentifierMap.put(
            (String) contentHierarchy.get(JsonKey.IDENTIFIER), contentDialCodes);
      }

      List<Map<String, Object>> children =
          (List<Map<String, Object>>) contentHierarchy.get(JsonKey.CHILDREN);
      hierarchyDialCodeIdentifierMap.putAll(getDialCodeIdentifierMap(children));

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
          if (StringUtils.isNotBlank(hierarchyDialCodeMap.get(k))
              && !v.equalsIgnoreCase(hierarchyDialCodeMap.get(k))) {
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
        hierarchyDialCodeIdentifierMap.putAll(
            getDialCodeIdentifierMap((List<Map<String, Object>>) child.get(JsonKey.CHILDREN)));
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
        topics.addAll(getTopic((List<Map<String, Object>>) child.get(JsonKey.CHILDREN)));
      }
    }
    return topics;
  }

  @SuppressWarnings("unchecked")
  private void validateDialCodesWithReservedDialCodes(
      Set<String> dialCodes, Map<String, Object> textBookdata) {
    Map<String, Integer> reservedDialcodeMap =
        (Map<String, Integer>) textBookdata.get(JsonKey.RESERVED_DIAL_CODES);
    Set<String> invalidDialCodes = new HashSet<>();
    if (MapUtils.isNotEmpty(reservedDialcodeMap)) {
      Set<String> reservedDialCodes = reservedDialcodeMap.keySet();
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
    Map<Integer, List<String>> rowNumVsContentIdsMap = new HashMap<>();
    List<Map<String, Object>> rows = new ArrayList<>();

    String tocMapping = ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_INPUT_MAPPING);
    Map<String, Object> configMap =
        mapper.readValue(tocMapping, new TypeReference<Map<String, Object>>() {});

    Map<String, String> metadata = (Map<String, String>) configMap.get(JsonKey.METADATA);
    Map<String, String> hierarchy = (Map<String, String>) configMap.get(JsonKey.HIERARCHY);
    Map<String, String> linkedContent = (Map<String, String>) configMap.get(JsonKey.LINKED_CONTENT);
    int max_allowed_content_size =
        Integer.parseInt(linkedContent.get(JsonKey.MAX_ALLOWED_CONTENT_SIZE));
    String linkedContentKey = linkedContent.get(JsonKey.LINKED_CONTENT_COLUMN_KEY);

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
        validateMandatoryFields(record, hierarchy.entrySet(), i);
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
          List<String> contentIds =
              validateLinkedContentUrlAndGetContentIds(
                  max_allowed_content_size, linkedContentKey, record, i);
          rowNumVsContentIdsMap.put(i, contentIds);
          map.put(JsonKey.METADATA, recordMap);
          map.put(JsonKey.HIERARCHY, hierarchyMap);
          map.put(JsonKey.CHILDREN, contentIds);
          rows.add(map);
        }
      }

      result.put(JsonKey.FILE_DATA, rows);
      result.put(JsonKey.DIAL_CODES, dialCodes);
      result.put(JsonKey.TOPICS, topics);
      result.put(JsonKey.DIAL_CODE_IDENTIFIER_MAP, dialCodeIdentifierMap);
      result.put(JsonKey.LINKED_CONTENT, rowNumVsContentIdsMap);
    } catch (ProjectCommonException e) {
      throw e;
    } catch (Exception e) {
      throwServerErrorException(ResponseCode.errorProcessingFile);
    } finally {
      try {
        if (null != csvFileParser) csvFileParser.close();
      } catch (IOException e) {
        ProjectLogger.log(
            "TextbookTocActor:readAndValidateCSV : Exception occurred while closing stream",
            LoggerEnum.ERROR);
      }
    }
    return result;
  }

  private void validateMandatoryFields(
      CSVRecord record, Set<Entry<String, String>> tocMetadata, int rowNum) {
    for (Map.Entry<String, String> entry : tocMetadata) {
      if ((TEXTBOOK_NAME.equalsIgnoreCase(entry.getValue())
              || L1_TEXTBOOK_UNIT.equalsIgnoreCase(entry.getValue()))
          && StringUtils.isBlank(record.get(entry.getValue()))) {
        String message =
            "Mandatory parameter " + entry.getValue() + " is missing at row " + rowNum + ".";
        ProjectCommonException.throwClientErrorException(ResponseCode.customClientError, message);
      }
    }
  }

  private List<String> validateLinkedContentUrlAndGetContentIds(
      int max_allowed_content_size, String linkedContentKey, CSVRecord record, int rowNumber) {
    List<String> contentIds = new ArrayList<>();
    for (int i = 1; i <= max_allowed_content_size; i++) {
      String key = MessageFormat.format(linkedContentKey, i).trim();
      String contentId = null;
      try {
        contentId = validateLinkedContentUrlAndGetContentId(record.get(key), rowNumber);
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
      }
      if (StringUtils.isNotBlank(contentId)) {
        if (contentIds.contains(contentId)) {
          String message =
              MessageFormat.format(
                  ResponseCode.errorDuplicateLinkedContentUrl.getErrorMessage(),
                  record.get(key),
                  rowNumber);
          ProjectCommonException.throwClientErrorException(
              ResponseCode.errorDuplicateLinkedContentUrl, message);
        }
        contentIds.add(contentId);
      } else {
        break;
      }
    }
    return contentIds;
  }

  private String validateLinkedContentUrlAndGetContentId(String contentUrl, int rowNumber) {
    if (StringUtils.isNotBlank(contentUrl)) {
      String linkedContentBaseUrl =
          ProjectUtil.getConfigValue(JsonKey.SUNBIRD_LINKED_CONTENT_BASE_URL);
      String[] arr = linkedContentBaseUrl.split("/");
      String[] contentUrlArr = contentUrl.split("/");
      if ((arr.length + 1) != contentUrlArr.length) {
        throwInvalidLinkedContentUrl(contentUrl, rowNumber);
      } else {
        if (contentUrl.startsWith(linkedContentBaseUrl)) {
          return contentUrlArr[contentUrlArr.length - 1];
        } else {
          throwInvalidLinkedContentUrl(contentUrl, rowNumber);
        }
      }
    }
    return "";
  }

  private void throwInvalidLinkedContentUrl(String contentUrl, Object rowNumber) {
    String message =
        MessageFormat.format(
            ResponseCode.errorInvalidLinkedContentUrl.getErrorMessage(), contentUrl, rowNumber);
    ProjectCommonException.throwClientErrorException(
        ResponseCode.errorInvalidLinkedContentUrl, message);
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
    if (CollectionUtils.isEmpty(data)) {
      throw new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    } else {
      log(
          "Create Textbook - UpdateHierarchy input data : " + mapper.writeValueAsString(data),
          LoggerEnum.INFO);
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

      Map<String, Object> updateRequest = new HashMap<String, Object>();
      Map<String, Object> requestMap = new HashMap<String, Object>();
      Map<String, Object> dataMap = new HashMap<String, Object>();
      Map<String, Object> hierarchy = new HashMap<String, Object>();
      if (MapUtils.isNotEmpty(hierarchyData)) {
        hierarchy.putAll(hierarchyData);
      }
      dataMap.put(JsonKey.NODES_MODIFIED, nodesModified);
      dataMap.put(JsonKey.HIERARCHY, hierarchy);
      requestMap.put(JsonKey.DATA, dataMap);
      updateRequest.put(JsonKey.REQUEST, requestMap);

      log(
          "Create Textbook - UpdateHierarchy Request : " + mapper.writeValueAsString(updateRequest),
          INFO.name());
      return callUpdateHierarchyAndLinkDialCodeApi(
          tbId, updateRequest, nodesModified, (String) textBookdata.get(JsonKey.CHANNEL));
    }
  }

  private Response callUpdateHierarchyAndLinkDialCodeApi(
      String tbId,
      Map<String, Object> updateRequest,
      Map<String, Object> nodesModified,
      String channel)
      throws Exception {
    Response response = new Response();
    updateHierarchy(tbId, updateRequest);
    try {
      linkDialCode(nodesModified, channel);
    } catch (Exception ex) {
      ProjectLogger.log(
          "TextbookTocActor:callUpdateHierarchyAndLinkDialCodeApi : Exception occurred while linking dial code : "
              + ex);
      response
          .getResult()
          .put(
              JsonKey.ERROR_MSG,
              "Textbook hierarchy metadata got updated but, "
                  + ResponseCode.errorDialCodeLinkingFail.getErrorMessage());
    }
    response.getResult().put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
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
  private Response updateTextbook(
      Request request, Map<String, Object> textbookData, Map<String, Object> textbookHierarchy)
      throws Exception {
    Boolean linkContent =
        (boolean) ((Map<String, Object>) request.get(JsonKey.DATA)).get(JsonKey.LINKED_CONTENT);
    String channel = (String) textbookData.get(JsonKey.CHANNEL);
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
      Map<String, Object> hierarchyData = new HashMap<>();

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
      if (BooleanUtils.isTrue(linkContent)) {
        List<Map<String, Object>> hierarchyList =
            getParentChildHierarchy(
                tbId,
                (String) textbookData.get(JsonKey.NAME),
                (List<Map<String, Object>>) textbookHierarchy.get(JsonKey.CHILDREN));
        Map<String, Object> hierarchy = populateHierarchyDataForUpdate(hierarchyList, tbId);
        data.forEach(
            s -> {
              Map<String, Object> nodeData =
                  (Map<String, Object>) hierarchy.get(s.get(JsonKey.IDENTIFIER));
              if (MapUtils.isNotEmpty(nodeData)
                  && CollectionUtils.isNotEmpty((List<String>) s.get(JsonKey.CHILDREN))) {
                ((List<String>) nodeData.get(JsonKey.CHILDREN))
                    .addAll((List<String>) s.get(JsonKey.CHILDREN));
              }
            });
        hierarchyData.putAll(hierarchy);
      }

      Map<String, Object> updateRequest = new HashMap<String, Object>();
      Map<String, Object> requestMap = new HashMap<String, Object>();
      Map<String, Object> dataMap = new HashMap<String, Object>();

      dataMap.put(JsonKey.NODES_MODIFIED, nodesModified);
      dataMap.put(JsonKey.HIERARCHY, hierarchyData);
      requestMap.put(JsonKey.DATA, dataMap);
      updateRequest.put(JsonKey.REQUEST, requestMap);
      log(
          "Update Textbook - UpdateHierarchy Request : " + mapper.writeValueAsString(updateRequest),
          INFO.name());
      return callUpdateHierarchyAndLinkDialCodeApi(
          (String) request.get(TEXTBOOK_ID), updateRequest, nodesModified, channel);
    }
  }

  public List<Map<String, Object>> getParentChildHierarchy(
      String parentId, String name, List<Map<String, Object>> children) {
    List<Map<String, Object>> hierarchyList = new ArrayList<>();
    Map<String, Object> hierarchy = new HashMap<>();
    Map<String, Object> node = new HashMap<>();
    node.put(JsonKey.NAME, name);
    List<String> contentIdList = new ArrayList<>();
    node.put(JsonKey.CHILDREN, contentIdList);
    hierarchy.put(parentId, node);
    hierarchyList.add(hierarchy);
    for (Map<String, Object> child : children) {
      contentIdList.add((String) child.get(JsonKey.IDENTIFIER));
      if (null != child.get(JsonKey.CHILDREN)) {
        hierarchyList.addAll(
            getParentChildHierarchy(
                (String) child.get(JsonKey.IDENTIFIER),
                (String) child.get(JsonKey.NAME),
                (List<Map<String, Object>>) child.get(JsonKey.CHILDREN)));
      }
    }
    return hierarchyList;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> populateHierarchyDataForUpdate(
      List<Map<String, Object>> hierarchy, String textbookId) {
    Map<String, Object> hierarchyData = new HashMap<>();
    hierarchy.forEach(
        s -> {
          for (Entry<String, Object> entry : s.entrySet()) {
            if (textbookId.equalsIgnoreCase(entry.getKey())) {
              ((Map<String, Object>) entry.getValue()).put(JsonKey.CONTENT_TYPE, "TextBook");
              ((Map<String, Object>) entry.getValue()).put(JsonKey.TB_ROOT, true);
            } else {
              ((Map<String, Object>) entry.getValue()).put(JsonKey.CONTENT_TYPE, "TextBookUnit");
              ((Map<String, Object>) entry.getValue()).put(JsonKey.TB_ROOT, false);
            }
            hierarchyData.put(entry.getKey(), entry.getValue());
          }
        });
    return hierarchyData;
  }

  @SuppressWarnings("unchecked")
  private void linkDialCode(Map<String, Object> modifiedNodes, String channel) throws Exception {
    List<Map<String, Object>> content = new ArrayList<>();
    modifiedNodes.forEach(
        (k, v) -> {
          Map<String, Object> value = (Map<String, Object>) v;
          Map<String, Object> metadata = (Map<String, Object>) value.get(JsonKey.METADATA);
          if (MapUtils.isNotEmpty(metadata)) {
            String dialCodeRequired = (String) metadata.get(JsonKey.DIAL_CODE_REQUIRED);
            if (JsonKey.YES.equalsIgnoreCase(dialCodeRequired)) {
              Map<String, Object> linkDialCode = new HashMap<>();
              linkDialCode.put(JsonKey.IDENTIFIER, k);
              if (null != metadata.get(JsonKey.DIAL_CODES)) {
                linkDialCode.put("dialcode", metadata.get(JsonKey.DIAL_CODES));
              } else {
                List<String> dialcodes = new ArrayList<>();
                linkDialCode.put("dialcode", dialcodes);
              }
              content.add(linkDialCode);
            }
          }
        });
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.CONTENT, content);
    Map<String, Object> linkDialCoderequest = new HashMap<>();
    linkDialCoderequest.put(JsonKey.REQUEST, request);
    if (CollectionUtils.isNotEmpty(content)) {
      linkDialCodeApiCall(linkDialCoderequest, channel);
    }
  }

  private Response linkDialCodeApiCall(Map<String, Object> updateRequest, String channel)
      throws Exception {
    String requestUrl =
        getConfigValue(JsonKey.EKSTEP_BASE_URL) + getConfigValue(JsonKey.LINK_DIAL_CODE_API);
    HttpResponse<String> updateResponse = null;
    try {
      Map<String, String> headers = getDefaultHeaders();
      headers.put("X-Channel-Id", channel);
      updateResponse =
          Unirest.post(requestUrl)
              .headers(headers)
              .body(mapper.writeValueAsString(updateRequest))
              .asString();
      if (null != updateResponse) {
        Response response = mapper.readValue(updateResponse.getBody(), Response.class);
        if (response.getResponseCode().getResponseCode() == ResponseCode.OK.getResponseCode()) {
          return response;
        } else {
          Map<String, Object> resultMap =
              Optional.ofNullable(response.getResult()).orElse(new HashMap<>());
          String message = "Linking of dial code failed ";
          if (MapUtils.isNotEmpty(resultMap)) {
            Object obj = Optional.ofNullable(resultMap.get(JsonKey.TB_MESSAGES)).orElse("");
            if (obj instanceof List) {
              message += ((List<String>) obj).stream().collect(Collectors.joining(";"));
            } else {
              message += String.valueOf(obj);
            }
          }
          ProjectCommonException.throwClientErrorException(
              ResponseCode.errorDialCodeLinkingClientError,
              MessageFormat.format(
                  ResponseCode.errorDialCodeLinkingClientError.getErrorMessage(), message));
        }
      } else {
        ProjectCommonException.throwClientErrorException(ResponseCode.errorDialCodeLinkingFail);
      }
    } catch (Exception ex) {
      if (ex instanceof ProjectCommonException) {
        throw ex;
      } else {
        throw new ProjectCommonException(
            ResponseCode.errorTbUpdate.getErrorCode(),
            ResponseCode.errorTbUpdate.getErrorMessage(),
            SERVER_ERROR.getResponseCode());
      }
    }
    return null;
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
      try {
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
              ResponseCode.errorDialCodeLinkingClientError.getErrorCode(),
              MessageFormat.format(
                  ResponseCode.errorDialCodeLinkingClientError.getErrorMessage(), message),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      } catch (Exception ex) {
        if (ex instanceof ProjectCommonException) {
          throw ex;
        } else {
          throw new ProjectCommonException(
              ResponseCode.errorTbUpdate.getErrorCode(),
              ResponseCode.errorTbUpdate.getErrorMessage(),
              SERVER_ERROR.getResponseCode());
        }
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
        "x-authenticated-user-token",
        "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ1WXhXdE4tZzRfMld5MG5PS1ZoaE5hU0gtM2lSSjdXU25ibFlwVVU0TFRrIn0.eyJqdGkiOiI1NjBmNWM2ZS0xYjc2LTRjYmItYjQ2Mi0yNTE5MzY1OGQ4N2UiLCJleHAiOjE1NDk2MzAyODIsIm5iZiI6MCwiaWF0IjoxNTQ5NjEyMjgyLCJpc3MiOiJodHRwczovL2Rldi5zdW5iaXJkZWQub3JnL2F1dGgvcmVhbG1zL3N1bmJpcmQiLCJhdWQiOiJhZG1pbi1jbGkiLCJzdWIiOiI4NzRlZDhhNS03ODJlLTRmNmMtOGYzNi1lMDI4ODQ1NTkwMWUiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJhZG1pbi1jbGkiLCJhdXRoX3RpbWUiOjAsInNlc3Npb25fc3RhdGUiOiIwN2ZlNjdmOC1jMDU3LTRmMDEtOWJkZi05OGM4YjY2YjQ3MDciLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnt9LCJuYW1lIjoiQ3JlYXRpb24iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJudHB0ZXN0MTAyIiwiZ2l2ZW5fbmFtZSI6IkNyZWF0aW9uIiwiZmFtaWx5X25hbWUiOiIiLCJlbWFpbCI6Imxha2hhbnNpbmdobWFuZGxvaTFAZ21haWwuY29tIn0.UQkHgUS4tf4vcfbKWr_6zghTtzg1CX8ZoKfFOGpKIxwSsfvPQW6jPNuWY65hmVIcnsbUTtNMC_R_IR7wBXoJXS7oh2Y6eIPp8u9ImaTM6L66zck8uQ3pIFIZgIDgpK_HniiKXYRbeDtvJyrTJf0-zDg8KJDfoZIhNTN5M72As2NIQj77HGpUBLEpVrqSTnCLNzQvH4ei4Ccp_scYDj7VD_NSVzh1xr_8VQTNnHa-ygSSwRPMmWONS0Lsr-Np_MOoYVr7vhXlj8o9TNDTp9AG4NoD1Veg2muezffZwyrriAnsYly2o4iI_PkLWc6f0KBXxX1_FDmSxT6e43liu8etzA");
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
    if (StringUtils.isNotBlank(code)) {
      nodesModified.put(code, node);
    }
  }

  private Map<String, Object> initializeMetaDataForModifiedNode(Map<String, Object> metadata) {
    Map<String, Object> newMeta = new HashMap<String, Object>();
    if (MapUtils.isNotEmpty(metadata)) {
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
