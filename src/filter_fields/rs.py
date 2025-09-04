import json
import os

input_file = os.path.expanduser('~/Documents/Vibe.Coding/Reddit-GenAI-Data-Platform/data/RS_reddit_raw.jsonl')
output_file = os.path.expanduser('~/Documents/Vibe.Coding/Reddit-GenAI-Data-Platform/data/RS_reddit.jsonl')

fields = [
    "id", "title", "selftext", "url", "permalink", "domain", "post_hint",
    "author", "author_fullname", "created_utc", "subreddit", "subreddit_id",
    "subreddit_name_prefixed", "subreddit_type", "subreddit_subscribers",
    "score", "num_comments", "total_awards_received", "edited", "locked",
    "spoiler", "over_18", "stickied", "retrieved_on", "is_original_content",
    "link_flair_text"
]

with open(input_file, 'r', encoding='utf-8') as file, \
     open(output_file, 'w', encoding='utf-8') as outfile:

    for l in file:
        try:
            data = json.loads(l)
            filtered_data = {key: data.get(key) for key in fields}
            json.dump(filtered_data, outfile)
            outfile.write('\n')
        except json.JSONDecodeError:
            continue 
