/**
 * 
 */
package org.sunbird.learner.util;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.PropertiesCache;

import com.google.gson.JsonObject;

/**
 * @author Manzarul
 *
 */
public class BadgingUtil {

	private BadgingUtil() {
	}

	/**
	 * This method will create assertion request data from requested map.
	 * 
	 * @param map
	 *            Map<String,Object>
	 * @return String
	 */
	public static String createAssertionReqData(Map<String, Object> map) {
		JsonObject json = new JsonObject();
		json.addProperty(JsonKey.RECIPIENT_IDENTIFIER, (String) map.get(JsonKey.RECIPIENT_EMAIL));
		json.addProperty(JsonKey.EVIDENCE, (String) map.get(JsonKey.EVIDENCE));
		json.addProperty(JsonKey.CREATE_NOTIFICATION, false);
		return json.toString();
	}

	public static String createAssertionUrl(Map<String, Object> map) {
		String uri =PropertiesCache.getInstance().getProperty("sunbird_badger_baseurl")+ MessageFormat.format(
				PropertiesCache.getInstance().getProperty("sunbird_badger_create_assertion_url"),
				(String) map.get(JsonKey.ISSUER_SLUG), (String) map.get(JsonKey.BADGE_CLASS_SLUG));
		return uri + "?format=json";
	}

	public static Map<String, String> createBadgerHeader() {
		Map<String, String> headermap = new HashMap<>();
		headermap.put("Authorization", "Token c6d0bdb8ce2425b26c2840bdca0f7b64e39be5fe");
		return headermap;
	}
}
