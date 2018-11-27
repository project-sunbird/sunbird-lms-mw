package org.sunbird.learner.service;

import java.util.Map;

public interface OrgService {

  boolean validateEmailUniqueness(Map<String, Object> orgMap, boolean isOrgCreate);
}
