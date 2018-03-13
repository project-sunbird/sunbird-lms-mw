package org.sunbird.badge.service;

import org.sunbird.badge.model.BadgeClassExtension;

import java.util.List;

public interface BadgeClassExtensionService {
    void save(BadgeClassExtension badgeClassExtension);
    List<BadgeClassExtension> get(String rootOrgId, String type, String subtype, List<String> roles);
    BadgeClassExtension get(String badgeId);
}
