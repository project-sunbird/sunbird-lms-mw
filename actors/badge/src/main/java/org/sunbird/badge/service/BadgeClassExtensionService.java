package org.sunbird.badge.service;

import org.sunbird.badge.model.BadgeClassExtension;

import java.util.List;

public interface BadgeClassExtensionService {
    void save(BadgeClassExtension badgeClassExtension);

    List<BadgeClassExtension> get(List<String> issuerList, String rootOrgId, String type, String subtype, List<String> roles);

    BadgeClassExtension get(String badgeId);

    void delete(String badgeId);
}
