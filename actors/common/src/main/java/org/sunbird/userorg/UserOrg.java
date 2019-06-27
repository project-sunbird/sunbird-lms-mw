package org.sunbird.userorg;

import org.sunbird.common.models.response.Response;
import org.sunbird.models.course.batch.CourseBatch;

import java.util.List;

public interface UserOrg {

    Response getOrganisationDetails(String orgID);

    Response getOrganisationDetailsForMultipleOrgIds(List<String> orgfields);

    Response getUserDetailsForSingleUserID(String userID);

    Response getUserDetailsForMultipleUserIDs(List<String> userIds);

}

