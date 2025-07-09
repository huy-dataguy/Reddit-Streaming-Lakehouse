from config import reddit
import json

def collect_comments(post_id = "1lv6gtm"):
    results = []

    submission = reddit.submission(post_id)
    submission.comments.replace_more(limit=None)

    for cmt in submission.comments[:10]:
        item = {
            "id": cmt.id,
            "subreddit": cmt.subreddit.display_name,
            "post_id": post_id,
            "author": str(cmt.author),
            "body": cmt.body,
            "created_utc": cmt.created_utc,
            "score": cmt.score,
            "depth": cmt.depth,
            "parent_id": cmt.parent_id,
            "is_submitter": cmt.is_submitter,
            "num_replies": len(cmt.replies)
            # "replies" : cmt.replies # fix them children cmt sau


        }
        results.append(item)

        with open("redditAPI/output/comments.json", "w") as f:
            json.dump(results, f, indent=2)

if __name__ == "__main__":
    collect_comments()

