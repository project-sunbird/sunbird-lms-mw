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


    private Error error;

    private CsvErrorDispatcher(Error error) {
        this.error = error;
    }

    public static CsvErrorDispatcher getInstance(Error error) {
        return new CsvErrorDispatcher(error);
    }

    @Override
    public void dispatchError() {
        Map<String, List<String>> errorMap = new HashMap<>();
        error.getErrorsList().stream().forEach(errorDetails -> {
            if (errorMap.containsKey(errorDetails.getHeader())) {
                List<String> errorRows = errorMap.get(errorDetails.getHeader());
                errorRows.add(errorDetails.getRowId() + "");
                errorMap.put(errorDetails.getHeader(), errorRows);
            } else {
                List<String> newErrorRows = new ArrayList<>();
                newErrorRows.add(errorDetails.getRowId() + "");
                errorMap.put(errorDetails.getHeader(), newErrorRows);
            }
        });
        throw new ProjectCommonException(
                ResponseCode.invalidRequestData.getErrorCode(),
                errorMap.toString(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
    }
}
