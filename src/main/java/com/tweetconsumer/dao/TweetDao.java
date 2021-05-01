package com.tweetconsumer.dao;

import com.tweetconsumer.model.Tweet;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TweetDao extends MongoRepository<Tweet,String> {
}
