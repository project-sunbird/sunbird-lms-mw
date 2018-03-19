package org.sunbird.badge.model;

import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;

import java.util.List;
import java.util.Map;

public class BadgeClassExtension {
    String badgeId;
    String issuerId;
    String rootOrgId;
    String type;
    String subtype;
    List<String> roles;

    public BadgeClassExtension(String badgeId, String issuerId, String rootOrgId, String type, String subtype, List<String> roles) {
        this.badgeId = badgeId;
        this.issuerId = issuerId;
        this.rootOrgId = rootOrgId;
        this.type = type;
        this.subtype = subtype;
        this.roles = roles;
    }

    public BadgeClassExtension(Map<String, Object> map) {
        this.badgeId = (String) map.get(JsonKey.ID);
        this.issuerId = (String) map.get(BadgingJsonKey.ISSUER_ID);
        this.rootOrgId = (String) map.get(JsonKey.ROOT_ORG_ID);
        this.type = (String) map.get(JsonKey.TYPE);
        this.subtype = (String) map.get(JsonKey.SUBTYPE);
        this.roles = (List<String>) map.get(JsonKey.ROLES);
    }

    public String getBadgeId() {
        return badgeId;
    }

    public void setBadgeId(String badgeId) {
        this.badgeId = badgeId;
    }

    public String getIssuerId() {
        return issuerId;
    }

    public void setIssuerId(String issuerId) {
        this.issuerId = issuerId;
    }

    public String getRootOrgId() {
        return rootOrgId;
    }

    public void setRootOrgId(String rootOrgId) {
        this.rootOrgId = rootOrgId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSubtype() {
        return subtype;
    }

    public void setSubtype(String subtype) {
        this.subtype = subtype;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }
}
