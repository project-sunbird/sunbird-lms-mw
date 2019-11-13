package org.sunbird.user.service;

import org.sunbird.common.models.response.Response;
import org.sunbird.dto.SearchDTO;
import org.sunbird.models.user.Feed;

public interface FeedService {


    Response save(Feed feed);
    Response search(SearchDTO searchDTO);
    void delete(String id);

}
