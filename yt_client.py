import json
import inspect
import requests
import time
from pprint import pprint
import argparse


class Client:
    def __init__(self, key):
        self.key = key
        self.url = 'https://www.googleapis.com/youtube/v3/'

    def get(self, method, params):
        url_params = [k + '=' + v for k, v in params.items() if v is not None]
        url_params.append('key=' + self.key)
        url_params.append('quotaUser=user1')
        resp = None
        exception = None
        for retry in range(10):
            try:
                time.sleep(1)
                resp = requests.get(self.url + method + '?' + '&'.join(url_params)).json()
                return resp
            except Exception as e:
                exception = e
        raise exception


class Comment:
    def __init__(self, id):
        self.id = id
        self.author = ''
        self.author_id = ''
        self.text = ''
        self.parent = None
        self.likes = 0
        self.moderation = ''
        self.published = ''
        self.updated = ''
        self.replies = []


def to_comment(json_data):
    comment = Comment(json_data['id'])
    try:
        comment.author = json_data['snippet']['authorDisplayName']
        comment.author_id = json_data['snippet']['authorChannelId']['value']
        comment.text = json_data['snippet']['textDisplay']
        comment.likes = json_data['snippet']['likeCount']
        comment.moderation = json_data['snippet'][
            'moderationStatus'] if 'moderationStatus' in json_data[
            'snippet'] else ''
        comment.published = json_data['snippet']['publishedAt']
        comment.updated = json_data['snippet']['updatedAt']
    except Exception as e:
        print(json_data)
        print(e)
    return comment


def to_comment_thread(json_data):
    top_level = to_comment(json_data['snippet']['topLevelComment'])
    top_level.replies = [to_comment(x) for x in
                         json_data['replies']['comments']] if 'replies' in json_data else []
    return top_level


class Video:
    def __init__(self, client, video_id):
        self._client = client
        self.id = video_id
        self.title = None
        self.info = None
        self.comments_threads = []
        self._comment_ids = set()
        try:
            restore(self, 'videos', self.id)
            cs = []
            for c in self.comments_threads:
                comment = Comment(None)
                to_object(comment, c)
                cs.append(comment)
            self.comments_threads = cs
        except Exception:
            pass

    def store(self):
        store(self, 'videos', self.id)

    def comments(self, remote=True):
        if not remote:
            return self.comments_threads
        page = None
        while True:
            curr_ids = set(x.id for x in self.comments_threads)
            resp = self._client.get('commentThreads',
                                    {'videoId': self.id, 'part': 'replies, snippet',
                                     'maxResults': '100',
                                     'pageToken': page})
            ids = set(x['snippet']['topLevelComment']['id'] for x in resp['items'])
            comments = {x['snippet']['topLevelComment']['id']: x for x in resp['items']}

            diff = ids.difference(curr_ids)
            if not diff:
                return self.comments_threads
            for id in diff:
                original = comments[id]
                comment = to_comment_thread(original)
                self.comments_threads.append(comment)

            if 'nextPageToken' not in resp:
                return self.comments_threads
            page = resp['nextPageToken']

    def get_info(self, remote=True):
        if not remote:
            return self.info

        resp = self._client.get('videos',
                                {'id': self.id, 'part': 'snippet,statistics'})
        self.info = {}
        if 'items' not in resp or len(resp['items']) == 0:
            raise Exception('no items in videos API response')
        self.info['snippet'] = resp['items'][0].get('snippet', {})
        self.info['statistics'] = resp['items'][0].get('statistics', {})
        return self.info


class Channel:
    def __init__(self, client, channel_id):
        self._client = client
        self.id = channel_id
        self.info = None
        self.upload_ids = []
        self.uploads_playlist_id = None
        try:
            restore(self, 'channels', self.id)
        except Exception:
            pass

    def store(self):
        store(self, 'channels', self.id)

    def uploads_playlist(self, remote=True):
        if self.uploads_playlist_id is None or remote:
            resp = self._client.get('channels',
                                    {'id': self.id, 'part': 'contentDetails,snippet,statistics'})
            if 'items' not in resp:
                pprint(resp)
            self.uploads_playlist_id = resp['items'][0]['contentDetails']['relatedPlaylists'][
                'uploads']
            self.info = dict()
            self.info['snippet'] = resp['items'][0]['snippet']
            self.info['statistics'] = resp['items'][0]['statistics']
        return self.uploads_playlist_id

    def uploads(self, remote=True):
        uploads_playlist_id = self.uploads_playlist()
        curr_uploads = set(self.upload_ids)

        if not remote:
            return [Video(self._client, x) for x in self.upload_ids]

        def contains(l1, l2):
            for i in l1:
                if i not in l2:
                    return False
            return True

        page = None
        while True:
            resp = self._client.get('playlistItems',
                                    {'playlistId': uploads_playlist_id, 'part': 'contentDetails',
                                     'maxResults': '50',
                                     'pageToken': page})
            ids = set(x['contentDetails']['videoId'] for x in resp['items'])
            if contains(ids, curr_uploads):
                self.upload_ids = list(curr_uploads)
                return [Video(self._client, x) for x in self.upload_ids]

            curr_uploads = curr_uploads.union(ids)
            if 'nextPageToken' not in resp:
                self.upload_ids = list(curr_uploads)
                return [Video(self._client, x) for x in self.upload_ids]
            page = resp['nextPageToken']


def update_channel_info(key, channel_id):
    client = Client(key)
    channel = Channel(client, channel_id)
    pprint(channel_id)
    remote = True
    uploads = channel.uploads(remote)
    channel.store()
    for upload in uploads:
        upload.get_info(remote)
        upload.store()


def store_comment_and_likes(key, f, channel):
    client = Client(key)
    channel = Channel(client, channel)
    remote = True
    uploads = channel.uploads(remote)
    all_top_level_comments = []
    for upload in uploads:
        comments = upload.comments(remote)
        for c in comments:
            all_top_level_comments.append([c.text, c.likes])
    with open(f, 'w') as fstore:
        json.dump(all_top_level_comments, fstore, indent=4, separators=(',', ': '))


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "to_json"):
            return self.default(obj.to_json())
        elif hasattr(obj, "__dict__"):
            d = dict(
                (key, value)
                for key, value in inspect.getmembers(obj)
                if not key.startswith("_")
                and not inspect.isabstract(value)
                and not inspect.isbuiltin(value)
                and not inspect.isfunction(value)
                and not inspect.isgenerator(value)
                and not inspect.isgeneratorfunction(value)
                and not inspect.ismethod(value)
                and not inspect.ismethoddescriptor(value)
                and not inspect.isroutine(value)
            )
            return self.default(d)
        return obj


def store(obj, category, name):
    with open('db/' + category + '/' + name, 'w') as fdb:
        json.dump(obj, fdb, cls=JsonEncoder)


def to_object(obj, d):
    for k, v in d.items():
        setattr(obj, k, v)


def restore(obj, category, name):
    with open('db/' + category + '/' + name) as fdb:
        data = json.load(fdb)
        to_object(obj, data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='YouTube client')
    parser.add_argument('--channel_id', '-c', required=True,
                        help='Channel ID')
    args = parser.parse_args()
    with open('api_key.txt') as fkey:
        key = fkey.read().strip()
    update_channel_info(key, args.channel_id)
