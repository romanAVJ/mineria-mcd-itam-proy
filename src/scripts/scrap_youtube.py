#!/usr/bin/env python
"""
6 sep 2023 12:00:00

@roman_avj
web scraping module
"""
# %% imports
from googleapiclient.discovery import build
import pandas as pd
import numpy as np
import re
import os
import json
from tqdm import tqdm
import time
# %% params
API_KEY = os.environ["YOUTUBE_API_KEY"]
CHANNEL_ID = "UCWVspRnU4QdsR4Av4tDWWRw" # oso trava's youtube channel id
FOLDER = "data/scrap_oso_trava"

# create folder
os.makedirs(FOLDER, exist_ok=True)

# %% =============================================================================
# FUNCTIONS
# =============================================================================
# channel stats
def get_channel_stats(youtube, channel_id):
    """
    Returns a dictionary with the channel's statistics
    """
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    return response

# channel playlists
def get_channel_playlists(youtube, channel_id):
    """
    Returns a dictionary with the channel's playlists
    """
    # get first request
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=50
    )
    response = request.execute()

    # iterate over the rest of the requests (page tokens)
    response_child = response
    while response_child.get("nextPageToken"):
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=response.get("nextPageToken")
        )
        response_child = request.execute()
        response["items"] = response["items"] + response_child["items"]


    # create a dataframe with the playlists (from the items)
    playlist_list = list()
    for r in response["items"]:
        # create dictionary
        playlist = dict()
        playlist["id"] = r.get("id")
        playlist["title"] = r["snippet"].get("title")
        playlist["description"] = r["snippet"].get("description")
        playlist["published_at"] = r["snippet"].get("publishedAt")
        playlist["video_count"] = r["contentDetails"].get("itemCount")
        playlist["channel_id"] = r["snippet"].get("channelId")
        playlist["photo_url"] = r["snippet"].get("thumbnails", dict()).get("maxres", dict()).get("url")
        # append to list
        playlist_list.append(playlist)
    
    # create dataframe
    playlist_df = pd.DataFrame(playlist_list)

    return playlist_df

# playlist videos
def get_playlist_videos(youtube, playlist_id):
    """
    Returns a dictionary with the playlist's videos
    """
    # get first request
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    # iterate over the rest of the requests (page tokens)
    response_child = response
    while response_child.get("nextPageToken"):
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=response.get("nextPageToken")
        )
        response_child = request.execute()
        response["items"] = response["items"] + response_child["items"]

    # create a dataframe with the videos (from the items)
    video_list = list()
    for r in response["items"]:
        # create dictionary
        video = dict()
        video["id"] = r["snippet"].get("resourceId", dict()).get("videoId")
        video["title"] = r["snippet"].get("title")
        video["description"] = r["snippet"].get("description")
        video["published_at"] = r["snippet"].get("publishedAt")
        video["playlist_id"] = r["snippet"].get("playlistId")
        video["channel_id"] = r["snippet"].get("channelId")
        video["photo_url"] = r["snippet"].get("thumbnails", dict()).get("maxres", dict()).get("url")
        # append to list
        video_list.append(video)

    # create dataframe
    video_df = pd.DataFrame(video_list)
    return video_df

# get video stats
def get_video_stats(youtube, video_id):
    """
    Returns a dictionary with the video's statistics
    """
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response = request.execute()

    # create dictionary
    video = dict()
    video["id"] = response["items"][0]["id"]
    video["tags"] = response["items"][0]["snippet"].get("tags")
    video["is_live"] = response["items"][0]["snippet"].get("liveBroadcastContent")
    video["duration"] = response["items"][0]["contentDetails"].get("duration")
    video["view_count"] = response["items"][0]["statistics"].get("viewCount")
    video["like_count"] = response["items"][0]["statistics"].get("likeCount")
    video["dislike_count"] = response["items"][0]["statistics"].get("dislikeCount")
    video["favorite_count"] = response["items"][0]["statistics"].get("favoriteCount")
    video["comment_count"] = response["items"][0]["statistics"].get("commentCount")

    return pd.Series(video)

# %% =============================================================================
# MAIN
# =============================================================================
# instantiate youtube api client
youtube = build('youtube', 'v3', developerKey=API_KEY)

# %% get channel stats
print("Getting channel stats...")
channel_stats = get_channel_stats(youtube, CHANNEL_ID)

# save data
with open(f"{FOLDER}/channel_stats.json", "w") as f:
    json.dump(channel_stats, f)

# %% get channel playlists
print("Getting channel playlists...")
df_playlists = get_channel_playlists(youtube, CHANNEL_ID)
# save data
df_playlists.to_parquet(f"{FOLDER}/channel_playlists.parquet")

# %% get playlist videos
print("Getting playlist videos...")
list_videos = list()
for playlist_id in tqdm(df_playlists["id"].unique()):
    df_videos = get_playlist_videos(youtube, playlist_id)
    list_videos.append(df_videos)
    time.sleep(1)

df_videos = pd.concat(list_videos)
# save data
df_videos.to_parquet(f"{FOLDER}/playlist_videos.parquet")

# %% get video stats
print("Getting video stats...")
list_video_stats = list()
for video_id in tqdm(df_videos["id"].unique()):
    video_stats = get_video_stats(youtube, video_id)
    list_video_stats.append(video_stats)
    time.sleep(1)
df_video_stats = pd.concat(list_video_stats)
# save data
df_video_stats.to_parquet(f"{FOLDER}/videos_stats.parquet")


