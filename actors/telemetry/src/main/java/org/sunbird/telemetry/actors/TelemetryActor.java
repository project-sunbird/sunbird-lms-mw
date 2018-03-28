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
import org.sunbird.util.lmaxdisruptor.LMAXWriter;

/**
 * @author Manzarul
 *
 */

@ActorConfig(tasks = { "saveTelemetry" }, asyncTasks = {})
public class TelemetryActor extends BaseActor {

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
		ProjectLogger.log("Saving telemetry data.", LoggerEnum.DEBUG.name());
		request.put(JsonKey.HEADER, createHeader(request));
		LMAXWriter.getInstance().submitMessage(request);
		sender().tell(new Response(), self());
	}

	private Map<String, String> createHeader(Request request) {
		Map<String, String> map = new HashMap<>();
		String authKey = System.getenv("ekstep_authorization");
		map.put("authorization", JsonKey.BEARER + authKey);
		if (request.getRequest().containsKey(JsonKey.FILE)) {
			map.put(JsonKey.CONTENT_ENCODING, "gzip");
		} else {
			map.put("Content-Type", "application/json");
		}
		return map;
	}

}
