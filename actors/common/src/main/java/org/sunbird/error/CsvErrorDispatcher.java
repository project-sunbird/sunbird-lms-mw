package org.sunbird.error;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.responsecode.ResponseCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this class will dispatch the errors in the csv format
 *
 * @author anmolgupta
 */
public class CsvErrorDispatcher implements IErrorDispatcher {


    private CsvError error;

    private CsvErrorDispatcher(CsvError error) {
        this.error = error;
    }

    public static CsvErrorDispatcher getInstance(CsvError error) {
        return new CsvErrorDispatcher(error);
    }

    @Override
    public void dispatchError() {
        Map<String, List<String>> errorMap = new HashMap<>();
        error.getErrorsList().parallelStream().forEach(errorDetails -> {
            List<String> errorRows = new ArrayList<>();
            if (errorMap.containsKey(errorDetails.getHeader())) {
                errorRows = errorMap.get(errorDetails.getHeader());
                errorRows.add(errorDetails.getRowId() + "");
            }
                errorRows.add(errorDetails.getRowId() + "");
                errorMap.put(errorDetails.getHeader(), errorRows);

        });
        throw new ProjectCommonException(
                ResponseCode.invalidRequestData.getErrorCode(),
                errorMap.toString(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
    }
}
