package org.sunbird.userorg;

import org.sunbird.common.models.response.Response;
import org.sunbird.models.course.batch.CourseBatch;

public interface UserOrg {

    Response getOragnisationDetails(String orgID);
}
