/**
 * 
 */
package org.sunbird.services.service;

import java.util.Map;

import org.sunbird.common.request.Request;



/**
 * This service will handle telemetry data to process it.
 * @author Manzarul 
 */
public interface TelemetryDataHandlerService {

	/**
	 * This method will take telemetry request data and it will call the
	 * implementation class to process it.
	 * 
	 * @param request
	 *            Request
	 * @param headers
	 *            Map<String,String>
	 */
	public void processData(Request request, Map<String, String> headers);
}
