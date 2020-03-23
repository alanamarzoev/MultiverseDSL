import dataflow 
from dataflow import * 

# HotCRP policies: --------------------------------------------

my_submitted_reviews = Filter("MySubmittedReviews", 
                              ["PaperReview"], 
                              ["$UID IN PaperReview.contactId"]) 

my_conflicts = Filter("MyConflicts", [
                      "PaperConflict"], 
                      ["$UID IN PaperConflict.contactID"], 
                      policy=True)

unconflicted_papers = Filter("UnconflictedPapers", 
                            ["Paper", "MyConflicts"], 
                            ["Paper.paperID NOT IN MyConflicts.paperId"],  
                            policy=True, 
                            exported_as="Paper")

unconflicted_paper_reviews = Filter("UnconflictedPaperReview", 
                                    ["PaperReview", "MyConflicts"], 
                                    ["PaperReview.paperID NOT IN MyConflicts.paperId"], 
                                    policy=True)

visible_reviews_unanonymized = Filter("VisibleReviews",
                                    ["UnconflictedPaperReview", "MySubmittedReviews"], 
                                    ["UnconflictedPaperReview.paperId IN MySubmittedReviews.paperId"], 
                                    policy=True) 

visible_reviews_anonymized = Transform("VisibleReviewsAnonymized", 
                                    ["VisibleReviews"], 
                                    ["VisibleReviews.contactID => `anonymous`"],
                                    policy=True, exported_as="PaperReview") 

hotcrp_policy_nodes = [my_submitted_reviews, my_conflicts, unconflicted_papers, unconflicted_paper_reviews, 
                        visible_reviews_unanonymized, visible_reviews_anonymized]

# HotCRP query: 

paper_paperreview = Filter("Paper_PaperReview", 
                            ["Paper", "PaperReview"],
                            ["PaperReview.paperId IN Paper.paperId", "PaperReview.contactId IN $UID"],
                            on=True, 
                            policy=False) 

r_submitted =  Aggregate("R_submitted",
                        "count(*)", 
                        ["PaperReview"], 
                        "PaperReview.paperId",
                        None, 
                        groupby="PaperReview.paperId", 
                        policy=False) # COUNT number of visible reviews 

final_join = Filter("Final", 
                    ["Paper_PaperReview", "R_submitted"], 
                    ["R_submitted.paperId IN Paper_PaperReview.paperId"], 
                    on=True, 
                    policy=False) # JOIN visible review count with rest of paper information 

hotcrp_query_nodes = [paper_paperreview, r_submitted, final_join]


# Twitter policies: --------------------------------------------

private_users = Filter("PrivateUsers", 
                      ["Users"], 
                      ["True IN Users.is_private"],
                      policy=True)

user_blocked_accounts = Filter("UserBlockedAccounts", 
                              ["BlockedAccounts"], 
                              ["$UID IN BlockedAccounts.user_id"],
                              policy=True)

user_blocked_by_accounts = Filter("UserBlockedByAccounts", 
                                 ["BlockedAccounts"], 
                                 ["UID IN BlockedAccounts.blocked_id"], 
                                 policy=True)

users_you_follow = Filter("UsersYouFollow", 
                          ["Follows"], 
                          ["UID IN Follows.user_id"], 
                          policy=True)

you_want_sensitive_tweets_marked = Filter("YouWantSensitiveTweetsMarked", 
                                         ["Users"], 
                                         ["UID IN Users.id", "True IN Users.is_marking_sensitive_content"], 
                                         policy=True)


visible_tweets1a = Filter("VisibleTweets1a", 
                        ["Tweets", "UsersYouFollow"], 
                        ["Tweets.user_id IN UsersYouFollow.user_id"], policy=True);  

visible_tweets1b = Filter("VisibleTweets1b", 
                        ["Tweets", "PrivateUsers"], 
                        ["Tweets.user_id NOT IN PrivateUsers"], policy=True)

visible_tweets1c = Filter("VisibleTweets1c", ["VisibleTweets1a", "VisibleTweets1b"], [], policy=True) 

visible_tweets = Filter("VisibleTweets", 
                        ["VisibleTweets1c", "UserBlockedAccounts", "UserBlockedByAccounts"], 
                        ["Tweets.user_id NOT IN UserBlockedAccounts",                         
                        "Tweets.user_id NOT IN UserBlockedByAccounts"], policy=True)


visible_and_marked_tweets = Transform("VisibleAndMarkedTweets", 
                                     ["VisibleTweets", "YouWantSensitiveTweetsMarked"], 
                                     ["$UID IN YouWantSensitiveTweetsMarked", "True IN VisibleTweets.is_sensitive",  "VisibleTweets.content => 'Marked as sensitive.'"], 
                                     policy=True, exported_as="Tweets")

twitter_policy_nodes = [private_users, user_blocked_accounts, user_blocked_by_accounts, users_you_follow, you_want_sensitive_tweets_marked, visible_tweets1a, 
                        visible_tweets1b, visible_tweets1c, visible_tweets, visible_and_marked_tweets]


# Twitter query: 

tweets_with_user_info = Filter("TweetsWithUserInfo", ["Tweets", "Users"], ["Tweets.user_id IN Users.id"], policy=False)
retweets = Filter("Retweets", ["TweetsWithUserInfo"], ["TweetsWithUserInfo.retweet_id IN TweetsWithUserInfo.rt_id"], policy=False) # WRONG: no edge from tweets with user info to retweets
all_tweets = Filter("AllTweets", ["TweetsWithUserInfo", "Retweets"], [], policy=False)

twitter_full_query = [tweets_with_user_info, retweets, all_tweets]