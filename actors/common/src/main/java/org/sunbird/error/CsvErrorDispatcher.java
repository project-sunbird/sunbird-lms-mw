package org.sunbird.error;

/**
 * this class will dispatch the errors in the csv format
 * @author anmolgupta
 */
public class CsvErrorDispatcher implements ErrorDispatcher {


    private Error error;

    private CsvErrorDispatcher(Error error) {
        this.error = error;
    }

    public static CsvErrorDispatcher getInstance(Error error) {
        return new CsvErrorDispatcher(error);
    }

    @Override
    public void dispatchError() {

    }
}
