package org.sunbird.models.user;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author anmolgupta
 * Pojo class for user_feed table.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Feed implements Serializable {


    private String id;
    private String userId;
    private String category;
    private int priority;
    private String createdBy;
    private int feedAction;
    private List<Map<String,Object>>feedData;
    private String updatedBy;
    private String channel;

    public Feed(Builder builder) {
        this.id = builder.id;
        this.userId = builder.userId;
        this.category = builder.category;
        this.priority = builder.priority;
        this.createdBy = builder.createdBy;
        this.feedAction = builder.feedAction;
        this.feedData = builder.feedData;
        this.updatedBy = builder.updatedBy;
        this.channel = builder.channel;
    }

    public String getId() {
        return id;
    }

    public String getUserId() {
        return userId;
    }

    public String getCategory() {
        return category;
    }

    public int getPriority() {
        return priority;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public int getFeedAction() {
        return feedAction;
    }

    public List<Map<String, Object>> getFeedData() {
        return feedData;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public String getChannel() {
        return channel;
    }

    public static class Builder{
        private String id;
        private String userId;
        private String category;
        private int priority;
        private String createdBy;
        private int feedAction;
        private List<Map<String,Object>>feedData;
        private String updatedBy;
        private String channel;

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setUserId(String userId) {
            this.userId = userId;
            return this;

        }

        public Builder setCategory(String category) {
            this.category = category;
            return this;

        }

        public Builder setPriority(int priority) {
            this.priority = priority;
            return this;

        }

        public Builder setCreatedBy(String createdBy) {
            this.createdBy = createdBy;
            return this;

        }

        public Builder setFeedAction(int feedAction) {
            this.feedAction = feedAction;
            return this;

        }

        public Builder setFeedData(List<Map<String, Object>> feedData) {
            this.feedData = feedData;
            return this;

        }

        public Builder setUpdatedBy(String updatedBy) {
            this.updatedBy = updatedBy;
            return this;

        }

        public Builder setChannel(String channel) {
            this.channel = channel;
            return this;

        }
        public Feed build(){
            Feed feed=new Feed(this);
            return feed;
        }
    }
}
