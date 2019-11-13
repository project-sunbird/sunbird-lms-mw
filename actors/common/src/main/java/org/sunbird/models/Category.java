package org.sunbird.models;

import org.sunbird.common.models.util.JsonKey;

public enum  Category {

    MIGRATION_USER(JsonKey.MIGRATION_USER_OBJECT);
    private String value;
    Category(String value) {
        this.value = value;
    }
    public String getValue() {
        return value;
    }
}
