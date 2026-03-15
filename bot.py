import feedparser
import requests
import time
import os

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

feeds = [
"https://www.iltalehti.fi/rss/nhl.xml",
"https://www.is.fi/rss/nhl.xml",
"https://www.tsn.ca/rss/nhl",
"https://www.sportsnet.ca/hockey/nhl/feed/"
]

seen=set()

def send(msg):
 url=f"https://api.telegram.org/bot{TOKEN}/sendMessage"
 requests.post(url,data={"chat_id":CHAT_ID,"text":msg})

while True:

 for feed_url in feeds:

  feed=feedparser.parse(feed_url)

  for entry in feed.entries:

   if entry.link not in seen:

    message=f"🚨 NHL NEWS\n\n{entry.title}\n{entry.link}"

    send(message)

    seen.add(entry.link)

 time.sleep(60)
