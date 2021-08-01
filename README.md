# Cari Maling
<p align="center">
   <a href="https://github.com/zxce3/cari-maling"><img src="https://user-images.githubusercontent.com/57337800/127140410-f5723561-85d5-46ad-a4c6-03ef55ab1533.jpg" alt="Cari Maling Logo" width=400px></a>
   <br>
   <br>
    <a href="https://github.com/Zxce3/cari-maling"><img src="https://img.shields.io/github/stars/Zxce3/cari-maling?style=social"></a>
  <a href="https://github.com/Zxce3/cari-maling/blob/cari-maling/LICENSE"><img src="https://img.shields.io/github/license/Zxce3/cari-maling?&style=social&logo=github"></a>
</p>


![VIEWS](https://komarev.com/ghpvc/?username=zxce3)
![Repo Size](https://img.shields.io/github/repo-size/Zxce3/cari-maling?&style=plastic&logo=github)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/zxce3/cari-maling?&style=plastic&logo=github)](https://github.com/Zxce3/cari-maling/graphs/commit-activity)
[![GitHub contributors](https://img.shields.io/github/contributors/Zxce3/cari-maling?&style=plastic&logo=github)](https://GitHub.com/Zxce3/cari-maling/graphs/contributors/)


Telegram Channel [@malingIT](https://t.me/malingIT)

* Index channel/group files for inline search.
* When you going to post file on telegram channel/group this bot will save that in database, So you can search that easily in inline mode.
* Supports document, video and audio file formats with caption.

### Installation

#### Easy Way
[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/Zxce3/cari-maling/tree/template)

#### Watch this video to create bot - https://youtu.be/dsuTn4qV2GA

#### Hard Way

```sh
python3 -m venv env
. ./env/bin/activate
pip3 install -r requirements.txt
# Edit info.py with variables as given below
python3 bot.py
```
Check `sample_info.py` before editing `info.py` file

#### Variables

##### Required Variables
* `BOT_TOKEN`: Create a bot using [@BotFather](https://telegram.dog/BotFather), and get the Telegram API token.
* `API_ID`: Get this value from [telegram.org](https://my.telegram.org/apps)
* `API_HASH`: Get this value from [telegram.org](https://my.telegram.org/apps)
* `CHANNELS`: Username or ID of channel or group. Separate multiple IDs by space
* `ADMINS`: Username or ID of Admin. Separate multiple Admins by space
* `DATABASE_URI`: [mongoDB](https://www.mongodb.com) URI. Get this value from [mongoDB](https://www.mongodb.com). For more help watch this [video](https://youtu.be/dsuTn4qV2GA)
* `DATABASE_NAME`: Name of the database in [mongoDB](https://www.mongodb.com). For more help watch this [video](https://youtu.be/dsuTn4qV2GA)

##### Optional Variables
* `COLLECTION_NAME`: Name of the collections. Defaults to Telegram_files. If you going to use same database, then use different collection name for each bot
* `MAX_RESULTS`: Maximum limit for inline search results
* `CACHE_TIME`: The maximum amount of time in seconds that the result of the inline query may be cached on the server
* `USE_CAPTION_FILTER`: Whether bot should use captions to improve search results. (True/False)
* `AUTH_USERS`: Username or ID of users to give access of inline search. Separate multiple users by space. Leave it empty if you don't want to restrict bot usage.

### Admin commands
```
channel - Get basic infomation about channels
total - Show total of saved files
delete - Delete file from database
logger - Get log file
```

### Tips
* Run [one_time_indexer.py](one_time_indexer.py) file to save old files in the database that are not indexed yet.
* You can use `|` to separate query and file type while searching for specific type of file. For example: `Avengers | video`
* If you don't want to create a channel or group, use your chat ID / username as the channel ID. When you send a file to a bot, it will be saved in the database.

### Contributions
Contributions are welcome.

### Thanks to
- [Pyrogram](https://github.com/pyrogram/pyrogram)
- [Mahesh0253](https://github.com/Mahesh0253)

### License
Code released under [The GNU General Public License](LICENSE).
