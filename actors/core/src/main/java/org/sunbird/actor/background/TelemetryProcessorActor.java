package org.sunbird.actor.background;

import java.util.Arrays;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.BackgroundRequestRouter;
import org.sunbird.common.request.Request;
import org.sunbird.telemetry.util.lmaxdisruptor.LMAXWriter;

/**
 * Created by arvind on 8/1/18.
 */
public class TelemetryProcessorActor extends BaseActor {

	private LMAXWriter lmaxWriter = LMAXWriter.getInstance();
	
	public static void init() {
		BackgroundRequestRouter.registerActor(TelemetryProcessorActor.class,
				Arrays.asList(BackgroundOperations.telemetryProcessing.name()));
	}

	@Override
	public void onReceive(Request request) throws Throwable {
		lmaxWriter.submitMessage(request);
	}
}
