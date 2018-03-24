/**
 * 
 */
package org.sunbird.telemetry.actors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.services.imp.TelemetryServiceFactory;
import org.sunbird.services.service.TelemetryDataHandlerService;

/**
 * @author Manzarul
 *
 */

@ActorConfig(tasks = { "saveTelemetry" }, asyncTasks = {})
public class TelemetryActor extends BaseActor {

	private TelemetryDataHandlerService service = TelemetryServiceFactory.getInstance();

	@Override
	public void onReceive(Request request) throws Throwable {
		ProjectLogger.log("TelemetryActor onReceive called", LoggerEnum.INFO.name());
		String operation = request.getOperation();
		switch (operation) {
		case "saveTelemetry":
			saveTelemetry(request);
			break;
		default:
			onReceiveUnsupportedOperation("TelemetryActor");
		}
	}

	/**
	 * This method will call the badger server to create badge assertion.
	 * 
	 * @param request
	 *            Request
	 */
	private void saveTelemetry(Request request) throws IOException {
		ProjectLogger.log("collecting telemetry data ", request.getRequest(), LoggerEnum.INFO.name());
		service.processData(request, createHeader());
		Response result = new Response();
		sender().tell(result, self());
	}

	
	private Map<String, String> createHeader() {
		Map<String, String> map = new HashMap<>();
		String authKey = System.getenv("ekstep_authorization");
		ProjectLogger.log("Telemetry auth value is comming as =" + authKey);
		map.put("Content-Type", "application/json");
		authKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJkNjNiMjgwZTQ1NDE0NDU4ODk4NzcwYzZhOGZiZjQ1MCJ9.Ji-22XcRrOiVy4dFAmE68wPxLkNmX4wKbTj_IB7fG6Y";
		map.put("authorization", JsonKey.BEARER + authKey);
		return map;
	}
	
	
}
