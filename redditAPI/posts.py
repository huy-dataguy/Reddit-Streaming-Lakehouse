from config import reddit
import json

def collect_posts(subreddit_name="technology", limit=1000):
    sub = reddit.subreddit(subreddit_name)
    categories = {
        "hot": sub.hot(limit=limit),
        "new": sub.new(limit=limit),
        "top": sub.top(limit=limit),
        "rising": sub.rising(limit=limit)
    }

    results = []

    for cat, submissions in categories.items():
        for submission in submissions:
            results.append({
                "type": cat,
                "subreddit": subreddit_name,
                "id": submission.id,
                "title": submission.title,
                "upvote_ratio" : submission.upvote_ratio,
                "ups" : submission.ups,
                "downs" : submission.downs,
                "score": submission.score,
                "link_flair_text" : submission.link_flair_text, # label
                "selftext" : submission.selftext,
                "url": submission.url,
                "created_utc": submission.created_utc,
                "num_comments": submission.num_comments,
                "author": str(submission.author),
            })

    with open("redditAPI/output/posts.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    collect_posts()
