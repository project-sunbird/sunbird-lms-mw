package org.sunbird.util.lmaxdisruptor;


import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.ProjectUtil;

import com.lmax.disruptor.EventHandler;

/**
 * This class will write data inside ES.
 * @author Manzarul
 *
 */
public class ESEventConsumer implements EventHandler<TelemetryEvent> {
	private static final String INDEX_NAME = "telemetry.raw";

	public void onEvent(TelemetryEvent writeEvent, long sequence, boolean endOfBatch) throws Exception {
		if (writeEvent != null && writeEvent.getData().getRequest() != null) {
		 ElasticSearchUtil.bulkInsertData(createIndex(), ProjectUtil.EsType.telemetry.getTypeName(),
					(List<Map<String,Object>>)writeEvent.getData().getRequest().getRequest().get("events"));
		}
	}

	/**
	 * This method will create index for Elastic search as follow
	 * "telemetry.raw.yyyy.mm"
	 * 
	 * @return
	 */
	private static String createIndex() {
		Calendar cal = Calendar.getInstance();
		return new StringBuffer().append(INDEX_NAME).append("." + cal.get(Calendar.YEAR))
				.append("." + ((cal.get(Calendar.MONTH) + 1) > 9 ? (cal.get(Calendar.MONTH) + 1)
						: "0" + (cal.get(Calendar.MONTH) + 1)))
				.toString();
	}
}
