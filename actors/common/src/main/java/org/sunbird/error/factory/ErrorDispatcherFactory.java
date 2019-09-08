package org.sunbird.error.factory;

import org.sunbird.error.CsvErrorDispatcher;
import org.sunbird.error.Error;
import org.sunbird.error.ErrorDispatcher;
import org.sunbird.error.ListErrorDispatcher;


/**
 * this is error dispatcher factory class which will judge type of error need to show on the basis of error count.
 *
 * @author anmolgupta
 */
public class ErrorDispatcherFactory {

    public static final int ERROR_VISUALIZATION_THRESHOLD = 100;

    public static ErrorDispatcher getErrorDispatcher(Error error) {
        if (error.getErrorsList().size() > ERROR_VISUALIZATION_THRESHOLD) {
            return CsvErrorDispatcher.getInstance(error);
        }
        return ListErrorDispatcher.getInstance(error);
    }

}
