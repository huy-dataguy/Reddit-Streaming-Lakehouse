from posts import collect_posts
from comments import collect_comments


def main():
    posts = collect_posts()
    for post in posts:
        collect_comments(post["id"])

if __name__ == "__main__":
    main()