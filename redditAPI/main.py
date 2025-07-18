from posts import collect_posts
from comments import collect_comments
import json
import os
import time

def main():
    posts = collect_posts(subreddit_name="technology", limit=25)

    with open("redditAPI/output/posts.json", "w") as f:
        json.dump(posts, f, indent=2)

    all_comments = []
    for post in posts:
        comments = collect_comments(post_id = post["id"])
        all_comments.extend(comments)
        time.sleep(1)
    
    with open("redditAPI/output/comments.json", "w") as f:
            json.dump(all_comments, f, indent=2)

if __name__ == "__main__":
    main()