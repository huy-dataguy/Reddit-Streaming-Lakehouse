import json
import os

input_file = os.path.expanduser('~/Documents/Vibe.Coding/Reddit-GenAI-Data-Platform/data/RC_reddit_raw.jsonl')
output_file = os.path.expanduser('~/Documents/Vibe.Coding/Reddit-GenAI-Data-Platform/data/RC_reddit.jsonl')

fields = [
    "id", "body", "created_utc", "edited", "score",
    "author", "author_fullname", "author_created_utc",
    "parent_id", "link_id", "is_submitter", "permalink",
    "subreddit", "subreddit_id", "subreddit_name_prefixed",
    "subreddit_type", "total_awards_received", "controversiality",
    "retrieved_on", "stickied"
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
