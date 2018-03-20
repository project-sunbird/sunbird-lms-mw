package org.sunbird.badge.service;

import java.util.List;

import org.sunbird.badge.model.BadgeClassExtension;

public interface BadgeClassExtensionService {
    void save(BadgeClassExtension badgeClassExtension);

    List<BadgeClassExtension> get(List<String> issuerList, String rootOrgId, String type, String subtype, List<String> roles);

    BadgeClassExtension get(String badgeId);

    void delete(String badgeId);
}
