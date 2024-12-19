# %%
from datetime import datetime, timedelta
import logging
import re

from dotenv import load_dotenv
import sqlite3 as sql
import pandas as pd
import spotipy as sp

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

load_dotenv()

sp = sp.Spotify(auth_manager=sp.oauth2.SpotifyOAuth(
    scope=','.join(['user-library-read'])
))


# %%
with sql.connect('spotify.sqlite') as cnx:
    try:
        df = pd.read_sql_query(
            'SELECT DISTINCT collected_at FROM saved_tracks', cnx)
        collected_at = datetime.strptime(
            df['collected_at'].loc[0], '%Y-%m-%d %H:%M:%S.%f')
    except pd.errors.DatabaseError as e:
        logger.info('No such table saved_tracks')
        collected_at = None

if collected_at is None or datetime.now() - timedelta(days=7) > collected_at:
    logger.info('Collecting liked songs')

saved_tracks = []
next_offset = 0
while True:
    response = sp.current_user_saved_tracks(limit=50, offset=next_offset)
    saved_tracks.extend(response['items'])
    next_url = response['next']
    if next_url is None:
        break

    next_offset = int(re.findall(r'offset=(\d+)', next_url)[0])

# %%
saved_tracks_df_raw = pd.json_normalize(saved_tracks, sep='.')
saved_tracks_df = pd.DataFrame({
    'id': saved_tracks_df_raw['track.id'],
    'name': saved_tracks_df_raw['track.name'],
    'popularity': saved_tracks_df_raw['track.popularity'],
    'duration_ms': saved_tracks_df_raw['track.duration_ms'],
    'explicit': saved_tracks_df_raw['track.explicit'],
    'album_id': saved_tracks_df_raw['track.album.id'],
    'album_name': saved_tracks_df_raw['track.album.name'],
    'album_release_date': saved_tracks_df_raw['track.album.release_date'],
    'album_type': saved_tracks_df_raw['track.album.album_type'],
    'artists': saved_tracks_df_raw['track.artists'].apply(
        lambda x: ','.join([artist['id'] for artist in x])),
    'added_at': saved_tracks_df_raw['added_at'],
    'collected_at': datetime.now(),
}).set_index('id')

saved_tracks_df['album_release_date'] = pd.to_datetime(
    saved_tracks_df['album_release_date'].apply(
        lambda d: d if re.match(r'^\d{4}-\d{2}-\d{2}$', d)
        else f'{d}-15' if re.match(r'^\d{4}-\d{2}$', d)
        else f'{d}-07-02' if re.match(r'^\d{4}$', d)
        else None
    )
)

# %%

with sql.connect('spotify.sqlite') as cnx:
    saved_tracks_df.to_sql(
        'saved_tracks', cnx, if_exists='replace', index=True, dtype={
            'collected': 'DATETIME',
            'added_at': 'DATETIME'
        })

artists = pd.json_normalize(saved_tracks_df_raw['track.artists'].explode())\
    .drop_duplicates(subset=['id'])\
    .set_index('id')
artists['collected_at'] = datetime.now()

with sql.connect('spotify.sqlite') as cnx:
    artists.to_sql('artists', cnx, if_exists='append', index=True)
    # Now remove duplicates in SQL table based on most recent collected at
    cnx.execute(
        'DELETE FROM artists WHERE rowid NOT IN '
        '(SELECT rowid FROM artists GROUP BY id ORDER BY collected_at DESC)')

# %%
with sql.connect('spotify.sqlite') as cnx:
    x = pd.read_sql_query('SELECT * FROM artists', cnx, index_col='id')
    y = pd.read_sql_query('SELECT * FROM saved_tracks', cnx, index_col='id')

# %%
