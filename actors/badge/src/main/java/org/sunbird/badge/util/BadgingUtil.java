/**
 * 
 */
package org.sunbird.badge.util;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;

import com.google.gson.JsonObject;

/**
 * @author Manzarul
 *
 */
public class BadgingUtil {
	public static final String  SUNBIRD_BADGER_CREATE_ASSERTION_URL="/v1/issuer/issuers/{0}/badges/{1}/assertions";
	public static final String  SUNBIRD_BADGER_GETASSERTION_URL="/v1/issuer/issuers/{0}/badges/{1}/assertion/{2}";
	public static final String  SUNBIRD_BADGER_GETALLASSERTION_URL="/v1/issuer/issuers/{0}/badges/{1}/assertion";
	public static final String  SUNBIRD_BADGER_REVOKE_URL="/v1/issuer/issuers/{0}/badges/{1}/assertion/{2}";
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
		json.addProperty(BadgingJsonKey.RECIPIENT_IDENTIFIER, (String) map.get(BadgingJsonKey.RECIPIENT_IDENTIFIER));
		if(!ProjectUtil.isStringNullOREmpty((String) map.get(BadgingJsonKey.EVIDENCE))) {
		json.addProperty(BadgingJsonKey.EVIDENCE, (String) map.get(BadgingJsonKey.EVIDENCE));
		}
		json.addProperty(BadgingJsonKey.CREATE_NOTIFICATION, false);
		return json.toString();
	}

	/**
	 * This method will create url for badger server call.
	 * in badger url pattern is first placeholder is issuerSlug, 2nd badgeclass slug
	 * and 3rd badgeassertion slug
	 * @param map  Map<String, Object>
	 * @param uri String
	 * @param placeholderCount int
	 * @return Stirng url
	 */
	public static String createBadgerUrl(Map<String, Object> map, String uri, int placeholderCount) {
		String url = PropertiesCache.getInstance().getProperty("sunbird_badger_baseurl")
				+ createUri(map, uri, placeholderCount);
		return url + "?format=json";
	}

	
	private static String createUri(Map<String, Object> map, String uri, int placeholderCount) {
		if (placeholderCount == 0) {
			return uri;
		} else if (placeholderCount == 1) {
			return MessageFormat.format(uri, (String) map.get(BadgingJsonKey.ISSUER_SLUG));
		} else if (placeholderCount == 2) {
			return MessageFormat.format(uri, (String) map.get(BadgingJsonKey.ISSUER_SLUG),
					(String) map.get(BadgingJsonKey.BADGE_SLUG));
		} else {
			return MessageFormat.format(uri, (String) map.get(BadgingJsonKey.ISSUER_SLUG),
					(String) map.get(BadgingJsonKey.BADGE_SLUG));
		}
	}
	
	
	public static Map<String, String> createBadgerHeader() {
		Map<String, String> headermap = new HashMap<>();
		headermap.put("Authorization", "Token c6d0bdb8ce2425b26c2840bdca0f7b64e39be5fe");
		headermap.put("Content-Type", "application/json");
		return headermap;
	}
	
}
