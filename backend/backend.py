from quart import Quart, request, jsonify
from os import getenv
from datetime import datetime, timedelta
from quart_cors import cors
from influxdb import InfluxDBClient
import requests
import asyncpg
import secrets

YOUTUBE = 'https://www.googleapis.com/youtube/v3/'

app = Quart(__name__)
app = cors(app)

pool: asyncpg.pool.Pool

influx = InfluxDBClient(getenv('INFLUX_IP'), 8086, database='violetwtf')

last_req = datetime.now() - timedelta(days=1)


def add_key(url: str) -> str:
    return url + f'&key={getenv("YT_API_KEY")}'


def get_url(channel_ids: list) -> str:
    return add_key(f'{YOUTUBE}channels?part=statistics,snippet&id={",".join(channel_ids)}')


def get_video_url(videos: list) -> str:
    return add_key(f'{YOUTUBE}videos?id={",".join(videos)}&part=statistics,snippet')


# Make this typed
async def acquire() -> asyncpg.Connection:
    return await pool.acquire()


@app.before_first_request
async def setup():
    global pool
    pool = await asyncpg.create_pool(dsn=getenv('WTF_PG_DSN'))

    con = await acquire()

    try:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS creators (
            id text PRIMARY KEY NOT NULL,
            subs int DEFAULT 0 NOT NULL,
            current bool DEFAULT TRUE NOT NULL,
            views_thousands int[] DEFAULT '{}'::int[] NOT NULL,
            video_ids text[] DEFAULT '{}'::text[] NOT NULL,
            name text NOT NULL,
            yt text NOT NULL,
            year_0 text NOT NULL,
            year_1 text,
            channel_id text
        )
        """)
        await con.execute("""
        CREATE TABLE IF NOT EXISTS tokens (
            token text PRIMARY KEY NOT NULL UNIQUE
        )
        """)
    finally:
        await pool.release(con)


@app.route('/creators')
async def get_creators():
    con = await acquire()
    final = {}

    try:
        res = await con.fetch('SELECT * FROM creators')

        for entry in res:
            subs_number = int(entry['subs'] / 1000)
            subs_text = 'thousand'

            if subs_number >= 1000:
                subs_number = round(subs_number / 1000, 1)
                subs_text = 'million'

            final[entry['id']] = {
                'subs': f'{subs_number} {subs_text}',
                'current': entry['current'],
                'viewsThousands': entry['views_thousands'],
                'videoIds': entry['video_ids'],
                'years': [entry['year_0'], entry['year_1']]
                if entry['year_1'] else [entry['year_0']],
                'yt': entry['yt'],
                'name': entry['name']
            }

        return final
    finally:
        await pool.release(con)


@app.route('/login', methods=['POST'])
async def login():
    json = (await request.get_json())
    print(json)
    if json == getenv("PASSWORD"):
        con = await acquire()
        try:
            token = secrets.token_hex(32)
            await con.execute('INSERT INTO tokens (token) VALUES ($1)', token)
        finally:
            await pool.release(con)

        return jsonify(token)
    return "false"


@app.route('/check', methods=['POST'])
async def check():
    json = (await (request.get_json()))

    if not json:
        return "false"

    return jsonify(await check_token(json))


@app.route('/videos', methods=['POST'])
async def add_video():
    json = (await (request.get_json()))

    if not json \
            or 'token' not in json \
            or 'id' not in json \
            or 'creator' not in json \
            or not await check_token(json['token']):
        return "false"

    con = await acquire()
    try:
        await con.execute(
            'UPDATE creators '
            'SET video_ids = array_append(video_ids, $1) '
            'WHERE id = $2',
            json['id'], json['creator']
        )
        global last_req
        last_req = datetime.now() - timedelta(days=1)
        await update_data(False)
    finally:
        await pool.release(con)

    return jsonify(True)


async def check_token(token: str) -> bool:
    con = await acquire()
    try:
        res = await con.fetchval('SELECT token FROM tokens WHERE token = $1', token)
    finally:
        await pool.release(con)
    return bool(res)


@app.before_request
async def update_data(do_stats: bool = True):
    global last_req

    now = datetime.now()

    if now - last_req > timedelta(minutes=10):
        last_req = now

        con = await acquire()
        try:
            res = await con.fetch(
                'SELECT id, channel_id, video_ids FROM creators'
            )

            channel_ids = []
            video_ids = []

            for creator in res:
                channel_ids.append(creator['channel_id'])
                video_ids += creator['video_ids']

            async with con.transaction():
                channel_json = requests.get(get_url(channel_ids)).json()
                video_json = {'items': []}

                count = 0
                video_ids_chunk = []

                for video_id in video_ids:
                    count = count + 1
                    video_ids_chunk.append(video_id)

                    if count == 50 or video_ids[-1] is video_id:
                        this_video_json = requests.get(get_video_url(video_ids_chunk)).json()

                        for item in this_video_json['items']:
                            video_json['items'].append(item)

                        count = 0
                        video_ids_chunk = []

                video_updates = {}

                json_body = []
                iso_time = datetime.now().isoformat()

                for channel in channel_json['items']:
                    await con.execute(
                        'UPDATE creators SET subs = $1 WHERE channel_id = $2',
                        int(channel['statistics']['subscriberCount']), channel['id']
                    )
                    video_updates[channel['id']] = []
                    json_body.append(
                        {
                            "measurement": "subs",
                            "tags":
                                {
                                    "id": channel['id'],
                                    "name": channel['snippet']['title']
                                },
                            "time": iso_time,
                            "fields":
                                {
                                    "value": int(channel['statistics']['subscriberCount'])
                                }
                        }
                    )

                for video in video_json['items']:
                    views = int(int(video['statistics']['viewCount']) / 1000)

                    # Hack to allow for multiple creators having the same video
                    for creator in res:
                        if video['id'] in creator['video_ids']:
                            video_updates[creator['channel_id']].append(views)

                    json_body.append(
                        {
                            "measurement": "views",
                            "tags":
                                {
                                    "id": video['id'],
                                    "title": video['snippet']['title'],
                                    "channelName": video['snippet']['channelTitle']
                                },
                            "time": iso_time,
                            "fields":
                                {
                                    "value": int(video['statistics']['viewCount'])
                                }
                        }
                    )

                for channel_id in video_updates:
                    await con.execute(
                        'UPDATE creators SET views_thousands = $1 WHERE channel_id = $2',
                        video_updates[channel_id],
                        channel_id
                    )

                if do_stats:
                    influx.write_points(json_body)
        finally:
            await pool.release(con)

