package org.sunbird.bean;

import org.sunbird.validator.user.UserBulkMigrationRequestValidator;

import java.util.Arrays;
import java.util.List;

public class Migration {

    private String fileSize;
    private List<String>headers;
    private byte[] fileData;
    private List<String>mandatoryFields;
    private List<String>supportedFields;
    private String processId;
    private List<MigrationUser>values;

    private Migration(MigrationBuilder migrationBuilder) {
        this.fileSize = migrationBuilder.fileSize;
        this.headers = migrationBuilder.headers;
        this.fileData = migrationBuilder.fileData;
        this.mandatoryFields=migrationBuilder.mandatoryFields;
        this.supportedFields=migrationBuilder.supportedFields;
        this.processId=migrationBuilder.processId;
        this.values=migrationBuilder.values;

    }
    public String getFileSize() {
        return fileSize;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public byte[] getFileData() {
        return fileData;
    }

    public List<String> getMandatoryFields() {
        return mandatoryFields;
    }

    public List<String> getSupportedFields() {
        return supportedFields;
    }

    public List<MigrationUser> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "Migration{" +
                "fileSize='" + fileSize + '\'' +
                ", headers=" + headers +
                ", fileData=" + Arrays.toString(fileData) +
                ", mandatoryFields=" + mandatoryFields +
                ", supportedFields=" + supportedFields +
                ", processId='" + processId + '\'' +
                ", values=" + values +
                '}';
    }


    public String getProcessId() {
        return processId;
    }


    public static class MigrationBuilder{

        private String fileSize;
        private List<String> headers;
        private byte[] fileData;
        private List<String>mandatoryFields;
        private List<String>supportedFields;
        private String processId;
        private List<MigrationUser>values;

        public MigrationBuilder() {
        }

        public MigrationBuilder setFileSize(String fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        public MigrationBuilder setHeaders(List<String> headers) {
            this.headers = headers;
            return this;
        }

        public MigrationBuilder setFileData(byte[] fileData) {
            this.fileData = fileData;
            return this;
        }
        public MigrationBuilder setMandatoryFields(List<String> mandatoryFields) {
            this.mandatoryFields = mandatoryFields;
            return this;
        }

        public MigrationBuilder setSupportedFields(List<String> supportedFields) {
            this.supportedFields = supportedFields;
            return this;
        }

        public MigrationBuilder setProcessId(String processId){
            this.processId=processId;
            return this;
        }

        public MigrationBuilder setValues(List<MigrationUser>values){
            this.values=values;
            return this;
        }

        public Migration build(){
            Migration migration=new Migration(this);
            validate(migration);
            return migration;
        }
        private void validate(Migration migration){
            UserBulkMigrationRequestValidator.getInstance(migration).validate();
        }
    }
}
