/**
 * 
 */
package org.sunbird.learner.util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.helper.ServiceFactory;

/**
 * This class will handle the data cache.
 * @author Amit Kumar
 */
public class DataCacheHandler implements Runnable{
	/**
	 * pageMap is the map of (orgId:pageName) and page Object (i.e map of string , object)
	 * sectionMap is the map of section Id and section Object (i.e map of string , object)
	 */
	private static Map<String, Map<String,Object>> pageMap = new ConcurrentHashMap<>();
	private static Map<String, Map<String,Object>> sectionMap = new ConcurrentHashMap<>();
	private static Map<String, Object> roleMap = new ConcurrentHashMap<>();
	CassandraOperation cassandraOperation = ServiceFactory.getInstance();
	
	@Override
	public void run() {
		ProjectLogger.log("Data cache started..");
		cache(pageMap,"page_management");
		cache(sectionMap,"page_section");
		roleCache(roleMap);
	}

	private void roleCache(Map<String, Object> roleMap) {
	  Response response = cassandraOperation.getAllRecords(Util.getProperty("db.keyspace"), JsonKey.ROLE_GROUP);
	  List<Map<String, Object>> responseList = (List<Map<String, Object>>)response.get(JsonKey.RESPONSE);
	  if(null != responseList && !responseList.isEmpty()){
        for(Map<String, Object> resultMap:responseList){
          roleMap.put((String)resultMap.get(JsonKey.ID), resultMap.get(JsonKey.NAME));
          }
       }
    }

  @SuppressWarnings("unchecked")
	private void cache(Map<String, Map<String, Object>> map,String tableName) {
		try{
			Response response = cassandraOperation.getAllRecords(Util.getProperty("db.keyspace"), tableName);
			List<Map<String, Object>> responseList = (List<Map<String, Object>>)response.get(JsonKey.RESPONSE);
			if(null != responseList && !responseList.isEmpty()){
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
			ProjectLogger.log(e.getMessage(), e);
		}
	}

  /**
   * @return the pageMap
   */
  public static Map<String, Map<String, Object>> getPageMap() {
    return pageMap;
  }

  /**
   * @param pageMap the pageMap to set
   */
  public static void setPageMap(Map<String, Map<String, Object>> pageMap) {
    DataCacheHandler.pageMap = pageMap;
  }

  /**
   * @return the sectionMap
   */
  public static Map<String, Map<String, Object>> getSectionMap() {
    return sectionMap;
  }

  /**
   * @param sectionMap the sectionMap to set
   */
  public static void setSectionMap(Map<String, Map<String, Object>> sectionMap) {
    DataCacheHandler.sectionMap = sectionMap;
  }

  /**
   * @return the roleMap
   */
  public static Map<String, Object> getRoleMap() {
    return roleMap;
  }

  /**
   * @param roleMap the roleMap to set
   */
  public static void setRoleMap(Map<String, Object> roleMap) {
    DataCacheHandler.roleMap = roleMap;
  }

}
