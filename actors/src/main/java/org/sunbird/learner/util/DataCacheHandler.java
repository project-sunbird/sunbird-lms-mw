/**
 * 
 */
package org.sunbird.learner.util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;

/**
 * This class will handle the data cache.
 * @author Manzarul
 * @author Amit Kumar
 */
public class DataCacheHandler implements Runnable{
	/**
	 * pageMap is the map of (orgId:pageName) and page Object (i.e map of string , object)
	 * sectionMap is the map of section Id and section Object (i.e map of string , object)
	 */
	public static ConcurrentHashMap<String, Map<String,Object>> pageMap = new ConcurrentHashMap<>();
	public static ConcurrentHashMap<String, Map<String,Object>> sectionMap = new ConcurrentHashMap<>();
	private static LogHelper logger = LogHelper.getInstance(DataCacheHandler.class.getName());
	CassandraOperation cassandraOperation = new CassandraOperationImpl();
	
	@Override
	public void run() {
		logger.info("Data cache started..");
		cache(pageMap,"page_management");
		cache(sectionMap,"page_section");
	}

	@SuppressWarnings("unchecked")
	private void cache(Map<String, Map<String, Object>> map,String tableName) {
		try{
			Response response = cassandraOperation.getAllRecords(Util.getProperty("db.keyspace"), tableName);
			List<Map<String, Object>> responseList = (List<Map<String, Object>>)response.get(JsonKey.RESPONSE);
			if(null != responseList && responseList.size() > 0){
				for(Map<String, Object> resultMap:responseList){
					if(tableName.equalsIgnoreCase(JsonKey.PAGE_SECTION)){
						map.put((String) resultMap.get(JsonKey.ID), resultMap);
					}else{
						String orgId = (((String) resultMap.get(JsonKey.ORGANISATION_ID)) == null ? "NA": (String) resultMap.get(JsonKey.ORGANISATION_ID));
						map.put(orgId+":"+((String) resultMap.get(JsonKey.PAGE_NAME)), resultMap);
					}
				}
			}
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}
	}

}
