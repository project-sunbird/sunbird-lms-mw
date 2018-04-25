package org.sunbird.learner.actors.bulkupload.model;

import java.io.Serializable;

/** Created by arvind on 24/4/18. */
public class BulkUpload implements Serializable {

  private static final long serialVersionUID = 1L;

  private String id;
  private String data;
  private String failureResult;
  private String objectType;
  private String organisationId;
  private String processEndTime;
  private String processStartTime;
  private Integer retryCount;
  private Integer status;
  private String successResult;
  private String uploadedBy;
  private String uploadedDate;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public String getFailureResult() {
    return failureResult;
  }

  public void setFailureResult(String failureResult) {
    this.failureResult = failureResult;
  }

  public String getObjectType() {
    return objectType;
  }

  public void setObjectType(String objectType) {
    this.objectType = objectType;
  }

  public String getOrganisationId() {
    return organisationId;
  }

  public void setOrganisationId(String organisationId) {
    this.organisationId = organisationId;
  }

  public Integer getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(Integer retryCount) {
    this.retryCount = retryCount;
  }

  public Integer getStatus() {
    return status;
  }

  public void setStatus(Integer status) {
    this.status = status;
  }

  public String getSuccessResult() {
    return successResult;
  }

  public void setSuccessResult(String successResult) {
    this.successResult = successResult;
  }

  public String getUploadedBy() {
    return uploadedBy;
  }

  public void setUploadedBy(String uploadedBy) {
    this.uploadedBy = uploadedBy;
  }

  public String getUploadedDate() {
    return uploadedDate;
  }

  public void setUploadedDate(String uploadedDate) {
    this.uploadedDate = uploadedDate;
  }

  public String getProcessEndTime() {
    return processEndTime;
  }

  public void setProcessEndTime(String processEndTime) {
    this.processEndTime = processEndTime;
  }

  public String getProcessStartTime() {
    return processStartTime;
  }

  public void setProcessStartTime(String processStartTime) {
    this.processStartTime = processStartTime;
  }

  public static class Builder {

    private String id;
    private String data;
    private String failureResult;
    private String objectType;
    private String organisationId;
    private String processEndTime;
    private String processStartTime;
    private Integer retryCount;
    private Integer status;
    private String successResult;
    private String uploadedBy;
    private String uploadedDate;

    // public Builder sugar(double cup){this.sugar = cup; return this; }

  }
}
