import mongoengine
from mongoengine import connect

class Post(mongoengine.Document):
    topic = mongoengine.StringField()
    influencer_name = mongoengine.StringField()
    date = mongoengine.StringField()
    title = mongoengine.StringField()
    image_url = mongoengine.StringField()
    video_url = mongoengine.StringField()

    def clean(self):
        self.topic = self.topic.strip()
        self.influencer_name = self.influencer_name.strip()
        self.date = self.date.strip()
        self.title = self.title.strip()
