from datetime import date, datetime, timedelta
import json
import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import pandas as pd
from google.cloud import storage

def retrieve_playlists(cid,secret,user,off):
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    #client_credentials_manager.get_access_token()
    ans = []
    for i in range(0,50,50): #change
        off = i
        playlists = sp.user_playlists(user, offset = off) #calculate offset with time
        for i, playlist in enumerate(playlists['items']):
            #print("%4d %s %s" % (i + 1 + playlists['offset'], playlist['uri'],  playlist['name']))
            ans.append((playlist['name'],user,playlist['uri'].split(':')[::-1][0]))
            if playlists['next']:
                playlists = sp.next(playlists)
        else:
            playlists = None
        off += 1
        return ans
    
def call_playlists(list_of_playlists, cid,secret):
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
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
            result = playlist_df.to_json(orient="split")
            out_file = open(f"{pl[0]}.json", "w")
            json.dump(result, out_file)
            out_file.close()
            
