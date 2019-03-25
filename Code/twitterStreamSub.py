from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

class StdOutListener(StreamListener):
    
    def on_data(self, data):
        print(data.rstrip())
        return True
    
    def on_error(self, status):
        print(status)
        
if __name__ == '__main__':
    l = StdOutListener()
    
    auth = OAuthHandler('CtBg0iWvaqKAoraAD16HGayxd', 'kWivTtMESss74xjMFqOV8hQQPPzbtnjfRBXuoZOJIhtJKIdr7f')
    auth.set_access_token('1109212604017328129-u7umZVz8B1ZdDIk1S63l5nvGOah8lz', 'gN1AkI5a5nQpz9PVljbbwfX90fCNwPNgQvSbdOZWHFt3U')
    
    stream = Stream(auth, l)
    stream.filter(track=['india'])
    