import praw
import os
from dotenv import load_dotenv

load_dotenv()

reddit = praw.Reddit(
    client_id= os.getenv('CLIENT_ID'),
    client_secret= os.getenv('CLIENT_SECRET'),
    user_agent = os.getenv('USER_AGENT')

)


# subreddit = reddit.subreddit("technology")

# for submission in subreddit.hot(limit=5):
#     print(vars(submission))
#     # print(submission.title)

