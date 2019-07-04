import os
import sys
import requests
import json
import tweepy

if sys.platform == 'darwin' or sys.platform == 'linux':
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bs4 import BeautifulSoup as bs
from mongoengine import connect
from tweet import Post

class TwitterScraper():
    def __init__(self):
        # get tweepy api key and secret from os environment
        CONSUMER_KEY = os.environ['CONSUMER_KEY']
        CONSUMER_SECRET = os.environ['CONSUMER_SECRET']
        ACCESS_TOKEN = os.environ['ACCESS_TOKEN']
        ACCESS_TOKEN_SECRET = os.environ['ACCESS_TOKEN_SECRET']
        # tweepy auth
        auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        self.__api = tweepy.API(auth)
        # connect client and db
        self.db = connect(db='twitter', host='localhost', port=5000)

    def get_trendings(self, max_topics=5):
        api = self.__api
        trendings = [{'topic': topic['name'], 'url': topic['url']} for topic in api.trends_place(1)[0]['trends'][:max_topics]]
        return trendings

    def crawl_trending_users(self, url, max_users):
        HEADERS = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8,ms;q=0.7,zh-CN;q=0.6,zh-TW;q=0.5,zh;q=0.4,ja;q=0.3",
            "cache-control": "max-age=0",
            "dnt": "1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"
        }

        with requests.Session() as session:
            response = session.get(url)
            cookies_infos = session.cookies.get_dict()
            cookies = '; '.join(['%s=%s' % (key, cookies_infos[key]) for key in cookies_infos.keys()])
            HEADERS['cookies'] = cookies

            page = requests.get(url, headers=HEADERS).content
            tweets = bs(page, 'html.parser').find('div', 'stream').find_all('div', 'content')[:max_users]
            users = ['https://twitter.com%s' % tweet.find('div', 'stream-item-header').find('a', 'account-group')['href'] for tweet in tweets]

        return users
    
    def get_trending_users(self, trendings, max_users=5):
        #trendings = self.get_trendings()
        user_groups = []
        for topic in trendings:
            topic_name = topic['topic']
            search_url = topic['url']
            users = self.crawl_trending_users(search_url, max_users)
            trend_data = {'topic': topic_name, 'user': users}
            user_groups.append(trend_data)
        return user_groups

    def crawl_users_profile(self, user_groups, num_of_tweets=40):
        #user_groups = self.get_trending_users()
        all_data = []
        for group in user_groups:
            topic = group['topic']
            user_urls = group['user']
            for url in user_urls:
                data = self.crawl(url, topic, num_of_tweets)
                all_data.extend(data)

        print('Crawling done, successfully crawled -> %i tweets' % len(all_data))
        return all_data

    def crawl(self, url, topic, num_of_tweets):
        all_data = []
        id_page = ''
        pages = int(num_of_tweets/20)
        influencer_name = url.split('/')[-1]

        for i in range(pages):
            goto = url + id_page
            #send GET requests to go to the page of 20 tweets where the first tweet is the id configured in the previous loop
            page = requests.get(goto)
            soup = bs(page.content, 'html.parser')

            containers	= soup.find_all('div', 'content')

            for content in containers:
                data = {}
                data['url'] = url
                data['topic'] = topic
                data['influencer_name'] = influencer_name
                title_container	= content.find('div', 'js-tweet-text-container')
                date_container	= content.find('small', 'time')
                media_container = content.find('div', 'AdaptiveMediaOuterContainer')

                if title_container:
                    for href in title_container.find_all('a', 'twitter-timeline-link u-hidden'):
                        href.decompose()
                    data['title'] = title_container.text
                    data['title'] = data['title'].replace('\n', '')

                if date_container:
                    tweet_id = date_container.find('a', 'tweet-timestamp js-permalink js-nav js-tooltip')['data-conversation-id']
                    data['date'] = date_container.find('a', 'tweet-timestamp js-permalink js-nav js-tooltip')['title']
                    data['date'] = data['date'].split(' - ')[1]

                if media_container:
                    images_src, vids_src = [], []
                    for img in media_container.find_all('img'):
                        images_src.append(img['src'])
                    for vid in media_container.find_all('div', 'PlayableMedia-player'):
                        vid_link = str("https://twitter.com/i/videos/tweet/" + tweet_id)
                        vids_src.append(vid_link)
                    data['image_url'] = images_src
                    data['video_url'] = vids_src

                if not date_container or not title_container:
                    continue

                all_data.append(data)
            #always get the tweet id for the last tweet in the page
            id_page	= soup.find('div', 'stream-container')['data-min-position']
            id_page	= '?max_position=' + id_page

        return all_data

    def save(self, data):
        post_data = []
        for row in data:
            try:
                if 'image_url' in row.keys() or 'video_url' in row.keys():
                    post = Post(topic=row['topic'], influencer_name=row['influencer_name'], date=row['date'],
                                title=row['title'], image_url=row['image_url'], video_url=row['video_url'])
                else:
                    post = Post(topic=row['topic'], influencer_name=row['influencer_name'], date=row['date'], title=row['title'])
            except KeyError:
                continue
            post_data.append(post)

        Post.objects().insert(post_data)
        print('Number of tweets loaded into db -> %i' % len(post_data))

if __name__ == "__main__":
    scraper = TwitterScraper()

    data = scraper.crawl_users_profile()
    scraper.save(data)
