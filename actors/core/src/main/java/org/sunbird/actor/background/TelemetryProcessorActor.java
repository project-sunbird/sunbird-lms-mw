package org.sunbird.actor.background;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.request.Request;
import org.sunbird.telemetry.util.lmaxdisruptor.LMAXWriter;

/**
 * Created by arvind on 8/1/18.
 */

@ActorConfig(tasks = {}, asyncTasks = { "telemetryProcessing" })
public class TelemetryProcessorActor extends BaseActor {

	private LMAXWriter lmaxWriter = LMAXWriter.getInstance();

	@Override
	public void onReceive(Request request) throws Throwable {
		lmaxWriter.submitMessage(request);
	}
}
