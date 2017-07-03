/**
 * 
 */
package org.sunbird.learner.util;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.sunbird.common.models.util.LogHelper;


/**
 * @author Manzarul
 *All the scheduler job will be handle by this class.
 */
public class SchedulerManager {
	 private static LogHelper logger = LogHelper.getInstance(Util.class.getName());
	 private static final int PAGE_DATA_TTL = 24;
	    /*
	     * service ScheduledExecutorService object
	     */
	    public static ScheduledExecutorService service = ExecutorManager
		    .getExecutorService();

	    /**
	     * all scheduler job will be configure here.
	     */
	    public static void schedule() {
	    logger.info("started scheduler job.");	
		service.scheduleWithFixedDelay(new DataCacheHandler(), 0, PAGE_DATA_TTL, TimeUnit.HOURS);
	    }

}
