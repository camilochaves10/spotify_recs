from datetime import date, datetime, timedelta
import json
import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import pandas as pd
from google.cloud import storage   
    
def retrieve_playlists(cid,secret,user):
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    #client_credentials_manager.get_access_token()
    ans = []
    dt = datetime.now()
    day = dt.day-10
    off = 0
    for i in range(0,1000, 50):
        print(i)
        playlists = sp.user_playlists(user, offset = off*day%26) #calculate offset with time
        for i, playlist in enumerate(playlists['items']):
            #print("%4d %s %s" % (i + 1 + playlists['offset'], playlist['uri'],  playlist['name']))
            ans.append((playlist['name'],user,playlist['uri'].split(':')[::-1][0]))
            print(ans[-1])
            if playlists['next']:
                playlists = sp.next(playlists)
        else:
            playlists = None
        off += 50
        #with open('spotify.pickle', 'wb') as f:
        #    pickle.dump(data, f)
    return ans


def call_playlists(list_of_playlists, cid,secret):
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    storage_client = storage.Client()
    final_df = pd.DataFrame()
    #step1
    for pl in list_of_playlists:
        playlist = sp.user_playlist_tracks(pl[1], pl[2])["items"]
        for track in playlist:
            # Create empty dict
            playlist_features = {}
            # Get metadata
            playlist_features["artist"] = track["track"]["album"]["artists"][0]["name"]
            playlist_features["album"] = track["track"]["album"]["name"]
            playlist_features["track_name"] = track["track"]["name"]
            playlist_features["track_id"] = track["track"]["id"]
            # Get audio features
            audio_features = sp.audio_features(playlist_features["track_id"])[0]
            for feature in playlist_features_list[4:]:
                playlist_features[feaure] = audio_features[feature]
            # Concat the dfs
            track_df = pd.DataFrame(playlist_features, index = [0])
            playlist_df = pd.concat([playlist_df, track_df], ignore_index = True)
            # dfs to json and dump
            result = playlist_df.to_json()
            final_df = pd.concat(final_df, result)
            # blob_list.append(f"{pl[0]}.json")
            # blob = bucket.blob(f"{pl[0]}.json")
            # with blob.open('w') as f:
            #     json.dump(result,f)
    return final_df
           
def write_csv_to_gcs(bucket_name, blob_name, service_account_key_file, df):
    """Write and read a blob from GCS using file-like IO"""
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        df.to_csv(f, index=False)