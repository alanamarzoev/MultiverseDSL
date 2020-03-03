-- MAIN TIMELINE

SELECT t.id t_id, t.timestamp t_created, t.content t_content, u.id u_id, u.handle u_handle, u.name u_name, t.reply_id t_r_id, retweets.*
FROM Tweets t 
  INNER JOIN Users u ON t.user_id = u.id -- Fetches user information about a tweet.
  LEFT JOIN ( -- Fetches retweet contents and its user information.
    SELECT t.id rt_id, t.timestamp rt_created, t.content rt_content, u.id rt_u_id, u.handle rt_u_handle, u.name rt_u_name
    FROM Tweets t
      INNER JOIN Users u ON t.user_id = u.id 
  ) retweets ON t.retweet_id = retweets.rt_id
WHERE t.user_id IN ( -- Limits to tweets/retweets/replies by user followed.
  SELECT followed_id FROM Follows WHERE user_id = ?)
ORDER BY t.timestamp DESC
LIMIT 20; -- Probably use pagination in reality.

--- USER TIMELINE 

user_timeline: SELECT t.id t_id, t.timestamp t_created, t.content t_content, u.id u_id, u.handle u_handle, u.name u_name, t.reply_id t_r_id, retweets.*
FROM Tweets t 
  INNER JOIN Users u ON t.user_id = u.id -- Fetches user information about a tweet.
  LEFT JOIN ( -- Fetches retweet contents and its user information.
    SELECT t.id rt_id, t.timestamp rt_created, t.content rt_content, u.id rt_u_id, u.handle rt_u_handle, u.name rt_u_name
    FROM Tweets t
      INNER JOIN Users u ON t.user_id = u.id 
  ) retweets ON t.retweet_id = retweets.rt_id
WHERE u.id = ?
ORDER BY t.timestamp DESC;


--- USER INFO 

user_info: SELECT * FROM Users WHERE id = ?;


--- DMs 

dms: SELECT * FROM Messages WHERE (sender_id = ? AND sendee_id = ??) OR (sender_id = ?? AND sendee_id = ?) ORDER BY timestamp DESC;


--- NOTIFS 
notifications: SELECT * FROM Notifications; 
