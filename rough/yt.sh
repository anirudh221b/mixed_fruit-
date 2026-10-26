#!/bin/bash

yt-dlp -x --no-playlist --audio-format mp3 $1 -o ./song_db/$2 
