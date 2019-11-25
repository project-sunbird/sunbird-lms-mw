package org.sunbird.feed.impl;

import org.sunbird.feed.IFeedService;

public class FeedFactory {
  private static IFeedService instance;

  public static IFeedService getInstance() {
    if (instance == null) {
      synchronized (FeedServiceImpl.class) {
        if (instance == null) instance = new FeedServiceImpl();
      }
    }
    return instance;
    // return new FeedServiceImpl();
  }
}
