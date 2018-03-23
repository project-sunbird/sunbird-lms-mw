/**
 * 
 */
package org.sunbird.services.imp;

import org.sunbird.services.service.TelemetryDataHandlerService;

/**
 * 
 * This factory will provide singleton object for
 * Telemetry DataHandler service.
 * @author Manzarul
 *
 */
public class TelemetryServiceFactory {

	private static TelemetryDataHandlerService service;

	static {
		service = new TelemetryDefaultHandlerServiceImpl();
	}

	/**
	 * Check if object is already created then return same , else create
	 * a new object and return it.
	 * @return TelemetryDataHandlerService
	 */
	public static TelemetryDataHandlerService getInstance() {
		if (service != null) {
			return service;
		}

		service = new TelemetryDefaultHandlerServiceImpl();
		return service;
	}
}
