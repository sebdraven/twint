import sys, os, time
from asyncio import get_event_loop, TimeoutError, ensure_future, new_event_loop, set_event_loop

from . import datelock, feed, get, output, verbose, storage
from .storage import db



class Twint:
    def __init__(self, config):
        if config.Resume is not None and (config.TwitterSearch or config.Followers or config.Following):
            self.init = self.get_resume(config.Resume)
        else:
            self.init = '-1'
            
        self.feed = [-1]
        self.count = 0
        self.user_agent = ""
        self.config = config
        self.conn = db.Conn(config.Database)
        self.d = datelock.Set(self.config.Until, self.config.Since)
        verbose.Elastic(config.Elasticsearch)

        if self.config.Store_object:
            output._clean_follow_list()

        if self.config.Pandas_clean:
            storage.panda.clean()

    def get_resume(self, resumeFile):
        if not os.path.exists(resumeFile):
            return '-1'
        with open(resumeFile, 'r') as rFile:
            _init = rFile.readlines()[-1].strip('\n')
            return _init

    async def Feed(self):
        consecutive_errors_count = 0
        while True:
            response = await get.RequestUrl(self.config, self.init, headers=[("User-Agent", self.user_agent)])
            if self.config.Debug:
                print(response, file=open("twint-last-request.log", "w", encoding="utf-8"))
                
            self.feed = []
            try:
                if self.config.Favorites:
                    self.feed, self.init = feed.Mobile(response)
                    if not self.count%40:
                        time.sleep(5)
                elif self.config.Followers or self.config.Following:
                    self.feed, self.init = feed.Follow(response)
                    if not self.count%40:
                        time.sleep(5)
                elif self.config.Profile:
                    if self.config.Profile_full:
                        self.feed, self.init = feed.Mobile(response)
                    else:
                        self.feed, self.init = feed.profile(response)
                elif self.config.TwitterSearch:
                    self.feed, self.init = feed.Json(response)
                break
            except TimeoutError as e:
                if self.config.Proxy_host.lower() == "tor":
                    print("[?] Timed out, changing Tor identity...")
                    if self.config.Tor_control_password is None:

                        sys.stderr.write("Error: config.Tor_control_password must be set for proxy autorotation!\r\n")
                        sys.stderr.write("Info: What is it? See https://stem.torproject.org/faq.html#can-i-interact-with-tors-controller-interface-directly\r\n")
                        break
                    else:
                        get.ForceNewTorIdentity(self.config)
                        continue
                else:

                    break
            except Exception as e:
                if self.config.Profile or self.config.Favorites:
                    print("[!] Twitter does not return more data, scrape stops here.")
                    break

                # Sometimes Twitter says there is no data. But it's a lie.
                consecutive_errors_count += 1
                if consecutive_errors_count < self.config.Retries_count:
                    self.user_agent = await get.RandomUserAgent()
                    continue
                break
        if self.config.Resume:
            print(self.init, file=open(self.config.Resume, "a", encoding="utf-8"))

    async def follow(self):
        await self.Feed()
        if self.config.User_full:
            self.count += await get.Multi(self.feed[0:self.config.Limit], self.config, self.conn)
        else:
            for user in self.feed[0:self.config.Limit]:
                self.count += 1
                username = user.find("a")["name"]
                await output.Username(username, self.config, self.conn)

    async def favorite(self):

        await self.Feed()
        self.count += await get.Multi(self.feed[0:self.config.Limit], self.config, self.conn)

    async def profile(self):
        await self.Feed()
        if self.config.Profile_full:
            self.count += await get.Multi(self.feed[0:self.config.Limit], self.config, self.conn)
        else:
            for tweet in self.feed[0:self.config.Limit]:
                self.count += 1
                await output.Tweets(tweet, self.config, self.conn)

    async def tweets(self):
        await self.Feed()
        if self.config.Location:
            self.count += await get.Multi(self.feed[0:self.config.Limit], self.config, self.conn)
        else:
            for tweet in self.feed[0:self.config.Limit]:
                self.count += 1
                await output.Tweets(tweet, self.config, self.conn)

    async def main(self, callback=None):

        task = ensure_future(self.run())  # Might be changed to create_task in 3.7+.

        if callback:
            task.add_done_callback(callback)

        await task

    async def run(self):
        if self.config.TwitterSearch:
            self.user_agent = await get.RandomUserAgent(wa=True)
        else:
            self.user_agent = await get.RandomUserAgent()



        if self.config.Username is not None:

            url = f"https://twitter.com/{self.config.Username}?lang=en"
            self.config.User_id = await get.User(url, self.config, self.conn, True)

        if self.config.User_id is not None:

            self.config.Username = await get.Username(self.config.User_id)

        if self.config.TwitterSearch and self.config.Since and self.config.Until:

            while self.d._since < self.d._until:
                self.config.Since = str(self.d._since)
                self.config.Until = str(self.d._until)
                if len(self.feed) > 0:
                    await self.tweets()
                else:
                    break

                if get.Limit(self.config.Limit, self.count):
                    break
        else:

            while True:
                if len(self.feed) > 0:
                    if self.config.Followers or self.config.Following:

                        await self.follow()
                    elif self.config.Favorites:

                        await self.favorite()
                    elif self.config.Profile:

                        await self.profile()
                    elif self.config.TwitterSearch:

                        await self.tweets()
                else:

                    break

                #logging.info("[<] " + str(datetime.now()) + ':: run+Twint+main+CallingGetLimit2')
                if get.Limit(self.config.Limit, self.count):

                    break

def run(config, callback=None):

    loop = new_event_loop()
    set_event_loop(loop)
    loop.run_until_complete(Twint(config).main(callback))

def Favorites(config):
    config.Favorites = True
    config.Following = False
    config.Followers = False
    config.Profile = False
    config.Profile_full = False
    config.TwitterSearch = False
    run(config)
    if config.Pandas_au:
        storage.panda._autoget("tweet")

def Followers(config):

    config.Followers = True
    config.Following = False
    config.Profile = False
    config.Profile_full = False
    config.Favorites = False
    config.TwitterSearch = False
    run(config)
    if config.Pandas_au:
        storage.panda._autoget("followers")
        if config.User_full:
            storage.panda._autoget("user")
    if config.Pandas_clean and not config.Store_object:
        #storage.panda.clean()
        output._clean_follow_list()

def Following(config):
    config.Following = True
    config.Followers = False
    config.Profile = False
    config.Profile_full = False
    config.Favorites = False
    config.TwitterSearch = False
    run(config)
    if config.Pandas_au:
        storage.panda._autoget("following")
        if config.User_full:
            storage.panda._autoget("user")
    if config.Pandas_clean and not config.Store_object:
        #storage.panda.clean()
        output._clean_follow_list()

def Lookup(config):
    if config.User_id is not None:
            config.Username = get_event_loop().run_until_complete(get.Username(config.User_id))
    url = f"https://twitter.com/{config.Username}?lang=en"
    get_event_loop().run_until_complete(get.User(url, config, db.Conn(config.Database)))
    if config.Pandas_au:
        storage.panda._autoget("user")

def Profile(config):
    config.Profile = True
    config.Favorites = False
    config.Following = False
    config.Followers = False
    config.TwitterSearch = False
    run(config)
    if config.Pandas_au:
        storage.panda._autoget("tweet")

def Search(config, callback=None):
    config.TwitterSearch = True
    config.Favorites = False
    config.Following = False
    config.Followers = False
    config.Profile = False
    config.Profile_full = False
    run(config, callback)
    if config.Pandas_au:
        storage.panda._autoget("tweet")
