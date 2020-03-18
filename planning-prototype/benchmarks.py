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
                                    policy=True, exported_as="PaperReview") # only see anonymized reviews for papers we've already submitted reviews for

hotcrp_policy_nodes = [my_submitted_reviews, my_conflicts, unconflicted_papers, unconflicted_paper_reviews, visible_reviews_unanonymized, visible_reviews_anonymized]


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

# private_users = Filter("PrivateUsers", ["Users"], ["True IN $Users.is_private"], affected_base_tables=["Users"], policy=True)

# user_blocked_accounts = Filter("UserBlockedAccounts", ["BlockedAccounts"], ["UID IN $BlockedAccounts.user_id"], affected_base_tables=["BlockedAccounts"], policy=True)

# user_blocked_by_accounts = Filter("UserBlockedByAccounts", ["BlockedAccounts"], ["UID IN $BlockedAccounts.blocked_id"], affected_base_tables=["BlockedAccounts"], policy=True)

# users_you_follow = Filter(["UsersYouFollow"], ["Follows"], ["UID IN Follows.user_id"], affected_base_tables=["Follows"], policy=True)

# you_want_sensitive_tweets_marked = Filter("YouWantSensitiveTweetsMarked", ["Users"], ["UID in Users.id", "True IN Users.is_marking_sensitive_content"], affected_base_tables=["Users"], policy=True)

# visible_tweets = Filter("VisibleTweets", ["Tweets", "UserBlockedByAccounts", "UsersBlockedAccounts"], 
#                                                   ["Tweets.user_id IN UsersYouFollow OR Tweets.user_id NOT IN PrivateUsers", "Tweets.user_id NOT IN UserBlockedAccounts", "Tweets.user_id NOT IN UserBlockedByAccounts"], 
#                                                    affected_base_tables=["Tweets"], policy=True)

# visible_and_marked_tweets = Transform("VisibleAndMarkedTweets", ["VisibleTweets", "YouWantSensitiveTweetsMarked"], 
#                                                                 ["UID IN YouWantSensitiveTweetsMarked", "True IN VisibleTweets.is_sensitive => VisibleTweets.content = 'Marked as sensitive.'"], 
#                                                                 affected_base_tables=["Tweets"], policy=True)


# # Twitter query: 

# tweets_with_user_info = Filter("TweetsWithUserInfo", ["Tweets", "Users"], ["Tweets.user_id = Users.id"], policy=False)
# retweets = Filter("Retweets", ["TweetsWithUserInfo"], ["TweetsWithUserInfo.retweet_id = TweetsWithUserInfo.rt_id"], policy=False)
