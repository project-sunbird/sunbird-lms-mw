package org.sunbird.learner.actors;

import static org.sunbird.common.models.util.ProjectUtil.isNotNull;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.RouterConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.TelemetryUtil;
import org.sunbird.learner.util.Util;

/**
 * This actor to handle learner's state update operation .
 *
 * @author Manzarul
 * @author Arvind
 */

@RouterConfig(request = {"addContent"}, bgRequest = {})
public class LearnerStateUpdateActor extends BaseActor {

	private static final String CONTENT_STATE_INFO = "contentStateInfo";

	private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

	/**
	 * Receives the actor message and perform the add content operation .
	 *
	 * @param request
	 *            Request
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void onReceive(Request request) throws Throwable {
		Util.initializeContext(request, JsonKey.USER);
		// set request id fto thread loacl...
		ExecutionContext.setRequestId(request.getRequestId());

		Response response = new Response();
		if (request.getOperation().equalsIgnoreCase(ActorOperations.ADD_CONTENT.getValue())) {
			Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
			Util.DbInfo batchdbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
			// objects of telemetry event...
			Map<String, Object> targetObject = null;
			List<Map<String, Object>> correlatedObject = null;

			String userId = (String) request.getRequest().get(JsonKey.USER_ID);
			List<Map<String, Object>> requestedcontentList = (List<Map<String, Object>>) request.getRequest()
					.get(JsonKey.CONTENTS);
			CopyOnWriteArrayList<Map<String, Object>> contentList = new CopyOnWriteArrayList<>(requestedcontentList);
			request.getRequest().put(JsonKey.CONTENTS, contentList);
			// map to hold the status of requested state of contents
			Map<String, Integer> contentStatusHolder = new HashMap<>();

			if (!(contentList.isEmpty())) {
				for (Map<String, Object> map : contentList) {
					// replace the course id (equivalent to Ekstep content id) with One way hashing
					// of
					// userId#courseId , bcoz in cassndra we are saving course id as userId#courseId

					String batchId = (String) map.get(JsonKey.BATCH_ID);
					boolean flag = true;

					// code to validate the whether request for valid batch range(start and end
					// date)
					if (!(ProjectUtil.isStringNullOREmpty(batchId))) {
						Response batchResponse = cassandraOperation.getRecordById(batchdbInfo.getKeySpace(),
								batchdbInfo.getTableName(), batchId);
						List<Map<String, Object>> batches = (List<Map<String, Object>>) batchResponse.getResult()
								.get(JsonKey.RESPONSE);
						if (batches.isEmpty()) {
							flag = false;
						} else {
							Map<String, Object> batchInfo = batches.get(0);
							flag = validateBatchRange(batchInfo);
						}

						if (!flag) {
							response.getResult().put((String) map.get(JsonKey.CONTENT_ID),
									"BATCH NOT STARTED OR BATCH CLOSED");
							contentList.remove(map);
							continue;
						}

					}
					map.putIfAbsent(JsonKey.COURSE_ID, JsonKey.NOT_AVAILABLE);
					preOperation(map, userId, contentStatusHolder);
					map.put(JsonKey.USER_ID, userId);
					map.put(JsonKey.DATE_TIME, new Timestamp(new Date().getTime()));

					try {
						cassandraOperation.upsertRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), map);
						response.getResult().put((String) map.get(JsonKey.CONTENT_ID), JsonKey.SUCCESS);
						// create telemetry for user for each content ...
						targetObject = TelemetryUtil.generateTargetObject((String) map.get(JsonKey.BATCH_ID),
								JsonKey.BATCH, JsonKey.CREATE, null);
						// since this event will generate multiple times so nedd to recreate correlated
						// objects every time ...
						correlatedObject = new ArrayList<>();
						TelemetryUtil.generateCorrelatedObject((String) map.get(JsonKey.CONTENT_ID), JsonKey.CONTENT,
								null, correlatedObject);
						TelemetryUtil.generateCorrelatedObject((String) map.get(JsonKey.COURSE_ID), JsonKey.COURSE,
								null, correlatedObject);
						TelemetryUtil.generateCorrelatedObject((String) map.get(JsonKey.BATCH_ID), JsonKey.BATCH, null,
								correlatedObject);
						TelemetryUtil.telemetryProcessingCall(request.getRequest(), targetObject, correlatedObject);

						Map<String, String> rollUp = new HashMap<>();
						rollUp.put("l1", (String) map.get(JsonKey.COURSE_ID));
						rollUp.put("l2", (String) map.get(JsonKey.CONTENT_ID));
						TelemetryUtil.addTargetObjectRollUp(rollUp, targetObject);

					} catch (Exception ex) {
						response.getResult().put((String) map.get(JsonKey.CONTENT_ID), JsonKey.FAILED);
						contentList.remove(map);
					}
				}
			}
			sender().tell(response, self());
			// call to update the corresponding course
			ProjectLogger.log("Calling background job to update learner state");
			request.getRequest().put(CONTENT_STATE_INFO, contentStatusHolder);
			request.setOperation(ActorOperations.UPDATE_LEARNER_STATE.getValue());
			tellToAnother(request);
		} else {
			onReceiveUnsupportedOperation(request.getOperation());
		}
	}

	private boolean validateBatchRange(Map<String, Object> batchInfo) {

		String start = (String) batchInfo.get(JsonKey.START_DATE);
		String end = (String) batchInfo.get(JsonKey.END_DATE);

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date todaydate = null;
		Date startDate = null;
		Date endDate = null;

		try {
			todaydate = format.parse((String) format.format(new Date()));
			startDate = format.parse(start);
			endDate = null;
			if (!(ProjectUtil.isStringNullOREmpty(end))) {
				endDate = format.parse(end);
			}
		} catch (ParseException e) {
			ProjectLogger.log("Date parse exception while parsing batch start and end date", e);
			return false;
		}

		if (todaydate.compareTo(startDate) < 0) {
			return false;
		}

		return (!(null != endDate && todaydate.compareTo(endDate) > 0));
	}

	/**
	 * Method te perform the per operation on contents like setting the status ,
	 * last completed and access time etc.
	 */
	@SuppressWarnings("unchecked")
	private void preOperation(Map<String, Object> req, String userId, Map<String, Integer> contentStateHolder)
			throws ParseException {

		SimpleDateFormat simpleDateFormat = ProjectUtil.getDateFormatter();
		simpleDateFormat.setLenient(false);

		Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
		req.put(JsonKey.ID, generatePrimaryKey(req, userId));
		contentStateHolder.put((String) req.get(JsonKey.ID), ((BigInteger) req.get(JsonKey.STATUS)).intValue());
		Response response = cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(),
				(String) req.get(JsonKey.ID));

		List<Map<String, Object>> resultList = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);

		if (!(resultList.isEmpty())) {
			Map<String, Object> result = resultList.get(0);
			int currentStatus = (int) result.get(JsonKey.STATUS);
			int requestedStatus = ((BigInteger) req.get(JsonKey.STATUS)).intValue();

			Integer currentProgressStatus = 0;
			if (isNotNull(result.get(JsonKey.CONTENT_PROGRESS))) {
				currentProgressStatus = (Integer) result.get(JsonKey.CONTENT_PROGRESS);
			}
			if (isNotNull(req.get(JsonKey.CONTENT_PROGRESS))) {
				Integer requestedProgressStatus = ((BigInteger) req.get(JsonKey.CONTENT_PROGRESS)).intValue();
				if (requestedProgressStatus > currentProgressStatus) {
					req.put(JsonKey.CONTENT_PROGRESS, requestedProgressStatus);
				} else {
					req.put(JsonKey.CONTENT_PROGRESS, currentProgressStatus);
				}
			} else {
				req.put(JsonKey.CONTENT_PROGRESS, currentProgressStatus);
			}

			Date accessTime = parseDate(result.get(JsonKey.LAST_ACCESS_TIME), simpleDateFormat);
			Date requestAccessTime = parseDate(req.get(JsonKey.LAST_ACCESS_TIME), simpleDateFormat);

			Date completedDate = parseDate(result.get(JsonKey.LAST_COMPLETED_TIME), simpleDateFormat);
			Date requestCompletedTime = parseDate(req.get(JsonKey.LAST_COMPLETED_TIME), simpleDateFormat);

			int completedCount;
			if (!(isNullCheck(result.get(JsonKey.COMPLETED_COUNT)))) {
				completedCount = (int) result.get(JsonKey.COMPLETED_COUNT);
			} else {
				completedCount = 0;
			}
			int viewCount;
			if (!(isNullCheck(result.get(JsonKey.VIEW_COUNT)))) {
				viewCount = (int) result.get(JsonKey.VIEW_COUNT);
			} else {
				viewCount = 0;
			}

			if (requestedStatus >= currentStatus) {
				req.put(JsonKey.STATUS, requestedStatus);
				if (requestedStatus == 2) {
					req.put(JsonKey.COMPLETED_COUNT, completedCount + 1);
					req.put(JsonKey.LAST_COMPLETED_TIME, compareTime(completedDate, requestCompletedTime));
				} else {
					req.put(JsonKey.COMPLETED_COUNT, completedCount);
				}
				req.put(JsonKey.VIEW_COUNT, viewCount + 1);
				req.put(JsonKey.LAST_ACCESS_TIME, compareTime(accessTime, requestAccessTime));
				req.put(JsonKey.LAST_UPDATED_TIME, ProjectUtil.getFormattedDate());

			} else {
				req.put(JsonKey.STATUS, currentStatus);
				req.put(JsonKey.VIEW_COUNT, viewCount + 1);
				req.put(JsonKey.LAST_ACCESS_TIME, compareTime(accessTime, requestAccessTime));
				req.put(JsonKey.LAST_UPDATED_TIME, ProjectUtil.getFormattedDate());
				req.put(JsonKey.COMPLETED_COUNT, completedCount);
			}

		} else {
			// IT IS NEW CONTENT SIMPLY ADD IT
			Date requestCompletedTime = parseDate(req.get(JsonKey.LAST_COMPLETED_TIME), simpleDateFormat);
			if (null != req.get(JsonKey.STATUS)) {
				int requestedStatus = ((BigInteger) req.get(JsonKey.STATUS)).intValue();
				req.put(JsonKey.STATUS, requestedStatus);
				if (requestedStatus == 2) {
					req.put(JsonKey.COMPLETED_COUNT, 1);
					req.put(JsonKey.LAST_COMPLETED_TIME, compareTime(null, requestCompletedTime));
					req.put(JsonKey.COMPLETED_COUNT, 1);
				} else {
					req.put(JsonKey.COMPLETED_COUNT, 0);
				}

			} else {
				req.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
				req.put(JsonKey.COMPLETED_COUNT, 0);
			}

			int progressStatus = 0;
			if (isNotNull(req.get(JsonKey.CONTENT_PROGRESS))) {
				progressStatus = ((BigInteger) req.get(JsonKey.CONTENT_PROGRESS)).intValue();
			}
			req.put(JsonKey.CONTENT_PROGRESS, progressStatus);

			req.put(JsonKey.VIEW_COUNT, 1);
			Date requestAccessTime = parseDate(req.get(JsonKey.LAST_ACCESS_TIME), simpleDateFormat);

			req.put(JsonKey.LAST_UPDATED_TIME, ProjectUtil.getFormattedDate());

			if (requestAccessTime != null) {
				req.put(JsonKey.LAST_ACCESS_TIME, (String) req.get(JsonKey.LAST_ACCESS_TIME));
			} else {
				req.put(JsonKey.LAST_ACCESS_TIME, ProjectUtil.getFormattedDate());
			}

		}
	}

	private Date parseDate(Object obj, SimpleDateFormat formatter) throws ParseException {
		if (null == obj || ((String) obj).equalsIgnoreCase(JsonKey.NULL)) {
			return null;
		}
		Date date;
		try {
			date = formatter.parse((String) obj);
		} catch (ParseException ex) {
			ProjectLogger.log(ex.getMessage(), ex);
			throw new ProjectCommonException(ResponseCode.invalidDateFormat.getErrorCode(),
					ResponseCode.invalidDateFormat.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());

		}
		return date;
	}

	private String compareTime(Date currentValue, Date requestedValue) {
		SimpleDateFormat simpleDateFormat = ProjectUtil.getDateFormatter();
		simpleDateFormat.setLenient(false);
		if (currentValue == null && requestedValue == null) {
			return ProjectUtil.getFormattedDate();
		} else if (currentValue == null) {
			return simpleDateFormat.format(requestedValue);
		} else if (null == requestedValue) {
			return simpleDateFormat.format(currentValue);
		}
		return (requestedValue.after(currentValue) ? simpleDateFormat.format(requestedValue)
				: simpleDateFormat.format(currentValue));
	}

	private String generatePrimaryKey(Map<String, Object> req, String userId) {
		String contentId = (String) req.get(JsonKey.CONTENT_ID);
		String courseId = (String) req.get(JsonKey.COURSE_ID);
		String batchId = (String) req.get(JsonKey.BATCH_ID);
		String key = userId + JsonKey.PRIMARY_KEY_DELIMETER + contentId + JsonKey.PRIMARY_KEY_DELIMETER + courseId
				+ JsonKey.PRIMARY_KEY_DELIMETER + batchId;
		return OneWayHashing.encryptVal(key);
	}

	private boolean isNullCheck(Object obj) {
		return null == obj;
	}

}
