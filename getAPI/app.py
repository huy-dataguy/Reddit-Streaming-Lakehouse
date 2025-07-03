import praw
import os
from dotenv import load_dotenv

load_dotenv()

reddit = praw.Reddit(
    client_id= os.getenv('CLIENT_ID'),
    client_secret= os.getenv('CLIENT_SECRET'),
    user_agent = os.getenv('USER_AGENT')

)
print(reddit.read_only)



for submission in reddit.subreddit("test").hot(limit=10):
    print(submission.title)


# subreddit = reddit.subreddit("technology")

# cnt = 0
# for submission in subreddit.hot(limit=100):
#     print(submission.title)
#     print(submission.score)
#     print(submission.id)
#     print(submission.url)
#     cnt+=1

# print(cnt)
