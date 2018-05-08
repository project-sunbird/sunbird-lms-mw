package org.sunbird.actor.core.bulkUpload;

import com.opencsv.CSVReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.BulkProcessStatus;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessDao;
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadProcessDaoImpl;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;

/** @author arvind. */
public abstract class BaseBulkUploadActor extends BaseActor {

  BulkUploadProcessDao bulkUploadDao = new BulkUploadProcessDaoImpl();

  public void validateBulkUploadFields(String[] csvHeaderLine, String[] bulkLocationAllowedFields) {

    if (ArrayUtils.isEmpty(csvHeaderLine)
        || ArrayUtils.isEmpty(bulkLocationAllowedFields)
        || !ArrayUtils.isSameLength(csvHeaderLine, bulkLocationAllowedFields)) {
      throw new ProjectCommonException(
          ResponseCode.invalidColumns.getErrorCode(),
          ResponseCode.invalidColumns.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode(),
          String.join(",", bulkLocationAllowedFields));
    }

    Arrays.stream(bulkLocationAllowedFields)
        .forEach(
            x -> {
              if (!(ArrayUtils.contains(csvHeaderLine, x))) {
                throw new ProjectCommonException(
                    ResponseCode.invalidColumns.getErrorCode(),
                    ResponseCode.invalidColumns.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode(),
                    String.join(",", bulkLocationAllowedFields));
              }
            });
  }

  public void validateBulkUploadAcceptableFields(
      String[] csvHeaderLine, String[] bulkLocationAllowedFields) {

    if (ArrayUtils.isEmpty(csvHeaderLine) || ArrayUtils.isEmpty(bulkLocationAllowedFields)) {
      throw new ProjectCommonException(
          ResponseCode.invalidColumns.getErrorCode(),
          ResponseCode.invalidColumns.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode(),
          String.join(",", bulkLocationAllowedFields));
    }

    Arrays.stream(csvHeaderLine)
        .forEach(
            x -> {
              if (!(ArrayUtils.contains(bulkLocationAllowedFields, x))) {
                throw new ProjectCommonException(
                    ResponseCode.invalidColumn.getErrorCode(),
                    ResponseCode.invalidColumn.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode(),
                    String.join(",", x));
              }
            });
  }

  public String[] trimColumnAttriutes(String[] columnArr) {
    for (int i = 0; i < columnArr.length; i++) {
      columnArr[i] = columnArr[i].trim();
    }
    return columnArr;
  }

  public BulkUploadProcess getBulkUploadProcessForFailedStatus(
      String processId, int status, Exception ex) {
    BulkUploadProcess bulkUploadProcess = new BulkUploadProcess();
    bulkUploadProcess.setId(processId);
    bulkUploadProcess.setStatus(status);
    bulkUploadProcess.setFailureResult(ex.getMessage());
    return bulkUploadProcess;
  }

  public CSVReader getCsvReader(byte[] byteArray, char seperator, char quoteChar, int lineNum) {

    InputStreamReader inputStreamReader =
        new InputStreamReader(new ByteArrayInputStream(byteArray));
    CSVReader csvReader = new CSVReader(inputStreamReader, seperator, quoteChar, lineNum);
    return csvReader;
  }

  public List<String[]> parseCsvFile(byte[] byteArray, String processId) throws IOException {
    CSVReader csvReader = null;
    // Create List for holding objects
    List<String[]> rows = new ArrayList<>();
    try {
      csvReader = getCsvReader(byteArray, ',', '"', 0);
      String[] strArray;
      // Read one line at a time
      while ((strArray = csvReader.readNext()) != null) {
        if (ProjectUtil.isNotEmptyStringArray(strArray)) {
          continue;
        }
        List<String> list = new ArrayList<>();
        for (String token : strArray) {
          list.add(token);
        }
        rows.add(list.toArray(list.toArray(new String[strArray.length])));
      }
    } catch (Exception ex) {
      ProjectLogger.log("Exception occured while processing csv file : ", ex);
      BulkUploadProcess bulkUploadProcess =
          getBulkUploadProcessForFailedStatus(processId, BulkProcessStatus.FAILED.getValue(), ex);
      bulkUploadDao.update(bulkUploadProcess);
      throw ex;
    } finally {
      try {
        IOUtils.closeQuietly(csvReader);
      } catch (Exception e) {
        ProjectLogger.log("Exception occured while closing csv reader : ", e);
      }
    }
    return rows;
  }

  public void validateFileSizeAgainstLineNumbers(int maxLines, int actualLines) {
    if (actualLines > 0 && actualLines > maxLines) {
      throw new ProjectCommonException(
          ResponseCode.dataSizeError.getErrorCode(),
          ProjectUtil.formatMessage(ResponseCode.dataSizeError.getErrorMessage(), maxLines),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  public void validateBulkUploadSize(List<String[]> csvLines) {

    if (null != csvLines) {
      if (csvLines.size() < 2) {
        throw new ProjectCommonException(
            ResponseCode.emptyFile.getErrorCode(),
            ResponseCode.emptyFile.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else {
      throw new ProjectCommonException(
          ResponseCode.emptyFile.getErrorCode(),
          ResponseCode.emptyFile.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }
}
