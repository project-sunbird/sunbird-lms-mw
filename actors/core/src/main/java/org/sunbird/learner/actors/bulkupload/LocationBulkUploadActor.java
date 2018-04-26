package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.ArrayUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BulkUploadActorOperation;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessDao;
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadProcessDaoImpl;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.util.Util;

/** Created by arvind on 23/4/18. */
@ActorConfig(
  tasks = {"locationBulkUpload"},
  asyncTasks = {}
)
public class LocationBulkUploadActor extends BaseActor {

  private static final String CSV_FILE_EXTENSION = ".csv";
  BulkUploadProcessDao bulkUploadDao = new BulkUploadProcessDaoImpl();
  String[] bulkLocationAllowedFields = {
    GeoLocationJsonKey.CODE,
    JsonKey.NAME,
    GeoLocationJsonKey.PARENT_CODE,
    GeoLocationJsonKey.PARENT_ID
  };

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, TelemetryEnvKey.GEO_LOCATION);
    ExecutionContext.setRequestId(request.getRequestId());
    String operation = request.getOperation();

    switch (operation) {
      case "locationBulkUpload":
        upload(request);
        break;
      default:
        onReceiveUnsupportedOperation("LocationBulkUploadActor");
    }
  }

  private void upload(Request request) throws IOException {

    String processId = ProjectUtil.getUniqueIdFromTimestamp(1);
    Map<String, Object> req = (Map<String, Object>) request.getRequest().get(JsonKey.DATA);
    String locationType = (String) req.get(GeoLocationJsonKey.LOCATION_TYPE);
    req.put(JsonKey.CREATED_BY, req.get(JsonKey.CREATED_BY));
    processLocationBulkUpload(req, processId, locationType);
  }

  private void processLocationBulkUpload(
      Map<String, Object> req, String processId, String locationType) throws IOException {
    File file = new File("bulk-" + processId + CSV_FILE_EXTENSION);
    List<String[]> csvLines = null;
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file);
      fos.write((byte[]) req.get(JsonKey.FILE));
      csvLines = parseCsvFile(file);
    } catch (IOException e) {
      ProjectLogger.log("Exception Occurred while reading file in BulkUploadManagementActor", e);
      throw e;
    } finally {
      try {
        if (ProjectUtil.isNotNull(fos)) {
          fos.close();
        }
        if (ProjectUtil.isNotNull(file)) {
          file.delete();
        }
      } catch (IOException e) {
        ProjectLogger.log(
            "Exception Occurred while closing fileInputStream in BulkUploadManagementActor", e);
      }
    }

    // validateBulkUploadSize(csvLines); // TODO : have move to controller side with file size
    // validation
    validateBulkUploadFields(csvLines.get(0), bulkLocationAllowedFields);
    // save csv file to db
    BulkUploadProcess bulkUploadProcess =
        uploadCsvToDB(
            csvLines,
            processId,
            JsonKey.LOCATION,
            (String) req.get(JsonKey.CREATED_BY),
            locationType);
    Response res = bulkUploadDao.create(bulkUploadProcess);

    sender().tell(res, self());

    if (((String) res.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      // send processId for data processing to background job
      Request request = new Request();
      request.put(JsonKey.PROCESS_ID, processId);
      request.setOperation(BulkUploadActorOperation.LOCATION_BULK_UPLOAD_BACKGROUND_JOB.getValue());
      tellToAnother(request);
    }
  }

  private BulkUploadProcess uploadCsvToDB(
      List<String[]> csvLines,
      String processId,
      String objectType,
      String requestedBy,
      String locationType) {

    List<Map<String, Object>> dataMapList = new ArrayList<>();
    if (csvLines.size() > 1) {
      try {
        String[] columnArr = csvLines.get(0);
        columnArr = trimColumnAttriutes(columnArr);
        Map<String, Object> dataMap = null;
        for (int i = 1; i < csvLines.size(); i++) {
          dataMap = new HashMap<>();
          String[] valueArr = csvLines.get(i);
          for (int j = 0; j < valueArr.length; j++) {
            String value = (valueArr[j].trim().length() == 0 ? null : valueArr[j].trim());
            dataMap.put(columnArr[j], value);
          }
          dataMap.put(GeoLocationJsonKey.LOCATION_TYPE, locationType);
          dataMapList.add(dataMap);
        }
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
        throw new ProjectCommonException(
            ResponseCode.csvError.getErrorCode(),
            ResponseCode.csvError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else {
      // tell sender that csv file is empty
      throw new ProjectCommonException(
          ResponseCode.csvError.getErrorCode(),
          ResponseCode.csvError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    // convert userMapList to json string
    Map<String, Object> map = new HashMap<>();

    ObjectMapper mapper = new ObjectMapper();
    try {
      map.put(JsonKey.DATA, mapper.writeValueAsString(dataMapList));
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }

    BulkUploadProcess bulkUploadProcess = new BulkUploadProcess();
    bulkUploadProcess.setId(processId);
    try {
      bulkUploadProcess.setData(mapper.writeValueAsString(dataMapList));
    } catch (IOException e) {
      // throw exception here... unale to parse file
      ProjectLogger.log(e.getMessage(), e);
    }

    bulkUploadProcess.setId(processId);
    bulkUploadProcess.setObjectType(objectType);
    bulkUploadProcess.setUploadedBy(requestedBy);
    bulkUploadProcess.setUploadedDate(ProjectUtil.getFormattedDate());
    bulkUploadProcess.setProcessStartTime(ProjectUtil.getFormattedDate());
    bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.NEW.getValue());
    return bulkUploadProcess;
  }

  private void validateBulkUploadSize(List<String[]> csvLines) {

    int allowedNoOfLines = 0;
    if (null != csvLines) {
      if (null != PropertiesCache.getInstance().getProperty(JsonKey.BULK_UPLOAD_ORG_DATA_SIZE)) {
        allowedNoOfLines =
            (Integer.parseInt(
                PropertiesCache.getInstance().getProperty(JsonKey.BULK_UPLOAD_ORG_DATA_SIZE)));
        ProjectLogger.log(
            "bulk location upload data size read from config file " + allowedNoOfLines);
      }
      if (csvLines.size() < 2 || csvLines.size() > allowedNoOfLines) {
        throw new ProjectCommonException(
            ResponseCode.dataSizeError.getErrorCode(),
            ProjectUtil.formatMessage(
                ResponseCode.dataSizeError.getErrorMessage(), allowedNoOfLines),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else {
      throw new ProjectCommonException(
          ResponseCode.dataSizeError.getErrorCode(),
          ProjectUtil.formatMessage(ResponseCode.dataSizeError.getErrorMessage(), allowedNoOfLines),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private void validateBulkUploadFields(
      String[] csvHeaderLine, String[] bulkLocationAllowedFields) {

    if (ArrayUtils.isEmpty(csvHeaderLine)
        || ArrayUtils.isEmpty(bulkLocationAllowedFields)
        || !ArrayUtils.isSameLength(csvHeaderLine, bulkLocationAllowedFields)) {
      throw new ProjectCommonException(
          ResponseCode.InvalidColumnError.getErrorCode(),
          ResponseCode.InvalidColumnError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    Arrays.stream(bulkLocationAllowedFields)
        .forEach(
            x -> {
              if (!(ArrayUtils.contains(csvHeaderLine, x))) {
                throw new ProjectCommonException(
                    ResponseCode.InvalidColumnError.getErrorCode(),
                    ResponseCode.InvalidColumnError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
              }
            });
  }

  private List<String[]> parseCsvFile(File file) {
    CSVReader csvReader = null;
    List<String[]> lines = new ArrayList<>();
    try {
      // Reading the csv file
      csvReader = new CSVReader(new FileReader(file), ',', '"', 0);
      String[] csvLine;
      while ((csvLine = csvReader.readNext()) != null) {
        if (ProjectUtil.isNotEmptyStringArray(csvLine)) {
          continue;
        }
        List<String> list = new ArrayList<>();
        for (String csvLineColumn : csvLine) {
          list.add(csvLineColumn);
        }
        lines.add(list.toArray(list.toArray(new String[csvLine.length])));
      }
    } catch (Exception e) {
      ProjectLogger.log("Exception occurred while processing csv file : ", e);
    } finally {
      try {
        // closing the reader
        csvReader.close();
        file.delete();
      } catch (Exception e) {
        ProjectLogger.log("Exception occurred while closing csv reader : ", e);
      }
    }
    return lines;
  }

  private String[] trimColumnAttriutes(String[] columnArr) {

    for (int i = 0; i < columnArr.length; i++) {
      columnArr[i] = columnArr[i].trim();
    }
    return columnArr;
  }
}
