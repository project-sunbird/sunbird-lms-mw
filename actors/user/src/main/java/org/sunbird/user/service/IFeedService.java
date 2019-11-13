package org.sunbird.user.service;

import org.sunbird.common.models.response.Response;
import org.sunbird.dto.SearchDTO;
import org.sunbird.models.user.Feed;

/**
 * @author anmolgupta
 * this is an interface class for the user feeds
 */
public interface IFeedService {


    /**
     * this method will be responsible to save the feed in the user_feed table and sync the data with the ES
     * @param feed
     * @return response
     */
    Response save(Feed feed);

    /**
     * this method will be holding responsibility to get and search the feed.
     * @param searchDTO
     * @return response
     */
    Response search(SearchDTO searchDTO);

    /**
     * this method will be holding responsibility to delete the feed.
     * @param id
     * @return true/false
     */
    boolean delete(String id);

}
