package org.sunbird.learner.actors.badging.service;

import org.sunbird.learner.actors.badging.model.BadgeClassExtension;

import java.util.List;

public interface BadgeClassExtensionService {
    void save(BadgeClassExtension badgeClassExtension);
    List<BadgeClassExtension> get(String rootOrgId, String type, String subtype, List<String> roles);
    BadgeClassExtension get(String badgeId);
}
