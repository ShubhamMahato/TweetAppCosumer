package com.tweetconsumer.model;


import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "TWEETS")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Tweet {
    @Transient
    private TweetEventType tweetEventType;
    private String username;
    private Integer likes;
    @Id
    private String id;
    private String message;
    private Date postDate;
    private List<Tweet> replyTweets;
    private boolean isLiked;
}
