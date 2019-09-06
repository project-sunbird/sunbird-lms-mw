package org.sunbird.error;

import java.util.List;

public class Error {

    private List<ErrorEnum> errorsList;


    public Error(List<ErrorEnum> errorsList) {
        this.errorsList = errorsList;
    }

    public Error() {
    }

    public List<ErrorEnum> getErrorsList() {
        return errorsList;
    }

    public void setErrorsList(List<ErrorEnum> errorsList) {
        this.errorsList = errorsList;
    }
}
