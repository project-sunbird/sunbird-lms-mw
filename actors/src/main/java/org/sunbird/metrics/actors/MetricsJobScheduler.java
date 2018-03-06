package org.sunbird.metrics.actors;

import java.util.Calendar;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;

public class MetricsJobScheduler implements Job {

	private static MetricsCache cache = new MetricsCache();

	public void execute(JobExecutionContext ctx) throws JobExecutionException {
		ProjectLogger.log("Running Metrics Job Scheduler at: " + Calendar.getInstance().getTime() + " triggered by: "
				+ ctx.getJobDetail().toString(), LoggerEnum.INFO.name());
		cache.clearCache();
		ProjectLogger.log("Clearing Cache for Dashboard API");
	}

}