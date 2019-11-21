package org.sunbird.models.user;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;

/** @author anmolgupta Pojo class for user_feed table. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Feed implements Serializable {
  private String id;
  private String userId;
  private String category;
  private int priority;
  private String createdBy;
  private String status;
  private Map<String, Object> data;
  private String updatedBy;
  private Timestamp expireOn;
  private Timestamp updatedOn;
  private Timestamp createdOn;

  public Feed(Builder builder) {
    this.id = builder.id;
    this.userId = builder.userId;
    this.category = builder.category;
    this.priority = builder.priority;
    this.createdBy = builder.createdBy;
    this.status = builder.status;
    this.data = builder.data;
    this.updatedBy = builder.updatedBy;
    this.expireOn = builder.expireOn;
    this.updatedOn = builder.updatedOn;
    this.createdOn = builder.createdOn;
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

  public String getStatus() {
    return status;
  }

  public Map<String, Object> getData() {
    return data;
  }

  public String getUpdatedBy() {
    return updatedBy;
  }

  public Timestamp getExpireOn() {
    return expireOn;
  }

  public Timestamp getUpdatedOn() {
    return updatedOn;
  }

  public Timestamp getCreatedOn() {
    return createdOn;
  }

  public static class Builder {
    private String id;
    private String userId;
    private String category;
    private int priority;
    private String createdBy;
    private String status;
    private Map<String, Object> data;
    private String updatedBy;
    private Timestamp expireOn;
    private Timestamp updatedOn;
    private Timestamp createdOn;

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

    public Builder setStatus(String status) {
      this.status = status;
      return this;
    }

    public Builder setData(Map<String, Object> Data) {
      this.data = data;
      return this;
    }

    public Builder setUpdatedBy(String updatedBy) {
      this.updatedBy = updatedBy;
      return this;
    }

    public void setExpireOn(Timestamp expireOn) {
      this.expireOn = expireOn;
    }

    public void setUpdatedOn(Timestamp updatedOn) {
      this.updatedOn = updatedOn;
    }

    public void setCreatedOn(Timestamp createdOn) {
      this.createdOn = createdOn;
    }

    public Feed build() {
      Feed feed = new Feed(this);
      return feed;
    }
  }
}
