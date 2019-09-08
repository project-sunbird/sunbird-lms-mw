package org.sunbird.error;

import java.util.ArrayList;
import java.util.List;

public class Error {

    private List<ErrorDetails> errorsList=new ArrayList<>();

    public Error() {
    }

    public List<ErrorDetails> getErrorsList() {
        return errorsList;
    }

    public void setErrorsList(List<ErrorDetails> errorsList) {
        this.errorsList = errorsList;
    }

    public void setError(ErrorDetails errorDetails){
        errorsList.add(errorDetails);
    }
}
