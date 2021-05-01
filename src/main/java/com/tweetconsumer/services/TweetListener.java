package com.tweetconsumer.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tweetconsumer.dao.TweetDao;
import com.tweetconsumer.model.Tweet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Slf4j
@Component
public class TweetListener {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    TweetDao tweetDao;

    @KafkaListener(topics = {"tweet-app"})
    public void onMessage(ConsumerRecord<String,String> consumerRecord){
        Tweet tweet=new Tweet();
        try {
            tweet=objectMapper.readValue(consumerRecord.value(),Tweet.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        switch (tweet.getTweetEventType()){
            case NEW:
                Tweet tweetToSave=Tweet.builder().id(tweet.getId()).message(tweet.getMessage()).postDate(tweet.getPostDate()).
                        replyTweets(tweet.getReplyTweets()).username(tweet.getUsername()).likes(null).build();
                tweetDao.save(tweetToSave);
                break;
            case UPDATE:
                updateTweet(tweet);
                break;
            case DELETE:
                deleteTweet(tweet);
                break;
            case REPLY:
                replyTweet(tweet);
            default:
                log.info("Not the message");

        }


        log.info("Consumer record: {}",consumerRecord);
    }
    public void updateTweet(Tweet tweet){
        Optional<Tweet> tweet1=tweetDao.findById(tweet.getId());
        if(tweet1.isPresent() && tweet.getMessage() !=null ) {
            Tweet tweetToSave = Tweet.builder().id(tweet1.get().getId()).message(tweet.getMessage()).postDate(tweet1.get().getPostDate()).
                    replyTweets(tweet1.get().getReplyTweets()).username(tweet1.get().getUsername()).likes(tweet1.get().getLikes()).build();
            tweetDao.save(tweetToSave);
        }
    }

    private void replyTweet(Tweet tweet){
        Optional<Tweet> tweet1=tweetDao.findById(tweet.getId());
        if(tweet1.isPresent()) {
            Tweet tweetToSave = Tweet.builder().id(tweet1.get().getId()).message(tweet1.get().getMessage()).postDate(tweet1.get().getPostDate()).
                    replyTweets(tweet1.get().getReplyTweets()).username(tweet1.get().getUsername()).likes(tweet1.get().getLikes()).build();
            tweetDao.save(tweetToSave);
        }
    }
    private void deleteTweet(Tweet tweet){
        log.info("START :: deleteTweet :: tweet :  {}",tweet);
        List<Tweet> tweetList=tweetDao.findAll();
        Tweet tweet1=new Tweet();
        for(Tweet t:tweetList){
            log.info(t.getId()+ "     tweet    "+ tweet.getId());
            if(t.getId().equalsIgnoreCase(tweet.getId())){
                tweet1=t;
            }
        }

        log.info("END :: deleteTweet :: deleted :  {}",tweet1);
        if(tweet1.getId()!=null) {
            tweetDao.delete(tweet1);
        }

    }

}
