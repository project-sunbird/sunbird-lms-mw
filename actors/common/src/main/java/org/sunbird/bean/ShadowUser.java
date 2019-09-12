package org.sunbird.bean;

import java.sql.Timestamp;
import java.util.List;

public class ShadowUser {

    /**
     *     channel text,
     *     claimedon timestamp,
     *     claimstatus int,
     *     createdon timestamp,
     *     email text,
     *     name text,
     *     orgextid text,
     *     phone text,
     *     processid text,
     *     roles list<text>,
     *     updatedon timestamp,
     *     userextid text,
     *     userid text,
     *     userstatus int
     */

    private Timestamp claimedOn;
    private int claimStatus;
    private Timestamp createdOn;
    private String email;
    private String name;
    private String orgExtId;
    private String phone;
    private String processId;
    private List<String> roles;
    private Timestamp updatedOn;
    private String userExtId;
    private String userId;
    private int userStatus;
    private String channel;
    private String addedBy;

    public ShadowUser() {
    }

    public ShadowUser(ShadowUserBuilder shadowUserBuilder) {
        this.claimedOn = shadowUserBuilder.claimedOn;
        this.claimStatus = shadowUserBuilder.claimStatus;
        this.createdOn = shadowUserBuilder.createdOn;
        this.email = shadowUserBuilder.email;
        this.name = shadowUserBuilder.name;
        this.orgExtId = shadowUserBuilder.orgExtId;
        this.phone = shadowUserBuilder.phone;
        this.processId = shadowUserBuilder.processId;
        this.roles = shadowUserBuilder.roles;
        this.updatedOn = shadowUserBuilder.updatedOn;
        this.userExtId = shadowUserBuilder.userExtId;
        this.userId = shadowUserBuilder.userId;
        this.userStatus = shadowUserBuilder.userStatus;
        this.channel=shadowUserBuilder.channel;
        this.addedBy=shadowUserBuilder.addedBy;
    }
    public Timestamp getClaimedOn() {
        return claimedOn;
    }

    public String getAddedBy() {
        return addedBy;
    }


    public int getClaimStatus() {
        return claimStatus;
    }

    public Timestamp getCreatedOn() {
        return createdOn;
    }

    public String getEmail() {
        return email;
    }

    public String getName() {
        return name;
    }

    public String getOrgExtId() {
        return orgExtId;
    }

    public String getPhone() {
        return phone;
    }

    public String getProcessId() {
        return processId;
    }

    public List<String> getRoles() {
        return roles;
    }

    public Timestamp getUpdatedOn() {
        return updatedOn;
    }

    public String getUserExtId() {
        return userExtId;
    }

    public String getUserId() {
        return userId;
    }

    public int getUserStatus() {
        return userStatus;
    }

    public String getChannel() {
        return channel;
    }

    public static class ShadowUserBuilder{

        private Timestamp claimedOn;
        private int claimStatus;
        private Timestamp createdOn;
        private String email;
        private String name;
        private String orgExtId;
        private String phone;
        private String processId;
        private List<String> roles;
        private Timestamp updatedOn;
        private String userExtId;
        private String userId;
        private int userStatus;
        private String channel;
        private String addedBy;

        public ShadowUserBuilder setAddedBy(String addedBy) {
            this.addedBy = addedBy;
            return this;
        }

        public ShadowUserBuilder setChannel(String channel) {
            this.channel = channel;
            return this;
        }

        public ShadowUserBuilder setClaimedOn(Timestamp claimedOn) {
            this.claimedOn = claimedOn;
            return this;

        }

        public ShadowUserBuilder setClaimStatus(int claimStatus) {
            this.claimStatus = claimStatus;
            return this;

        }

        public ShadowUserBuilder setCreatedOn(Timestamp createdOn) {
            this.createdOn = createdOn;
            return this;

        }

        public ShadowUserBuilder setEmail(String email) {
            this.email = email;
            return this;

        }

        public ShadowUserBuilder setName(String name) {
            this.name = name;
            return this;

        }

        public ShadowUserBuilder setOrgExtId(String orgExternalId) {
            this.orgExtId = orgExternalId;
            return this;

        }

        public ShadowUserBuilder setPhone(String phone) {
            this.phone = phone;
            return this;

        }

        public ShadowUserBuilder setProcessId(String processId) {
            this.processId = processId;
            return this;

        }

        public ShadowUserBuilder setRoles(List<String> roles) {
            this.roles = roles;
            return this;

        }

        public ShadowUserBuilder setUpdatedOn(Timestamp updatedOn) {
            this.updatedOn = updatedOn;
            return this;

        }

        public ShadowUserBuilder setUserExtId(String userExtId) {
            this.userExtId = userExtId;
            return this;

        }

        public ShadowUserBuilder setUserId(String userId) {
            this.userId = userId;
            return this;

        }

        public ShadowUserBuilder setUserStatus(int userStatus) {
            this.userStatus = userStatus;
            return this;
        }

        public ShadowUser build(){
            ShadowUser shadowUser=new ShadowUser(this);
            return shadowUser;
        }
    }
}
