import requests
import json 
import os
import redis 
from dotenv import load_dotenv
from multiprocessing import Pool, cpu_count
from pydub import AudioSegment
import yt_dlp
import time 

load_dotenv()

class DownloadReels: 
    def __init__(self):
        self.redisClient = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=0)
    
    def getFilePaths(self, redisClient, batch_size=10):
        filePaths = []
        for _ in range(batch_size):
            filePath = redisClient.lpop('unparsed_json_queue')
            if filePath:
                filePaths.append(filePath)
            else:
                break
        return filePaths
        
    def downloadReels(self, filePath): 
        try: 
            file_name_start = filePath.split('/')[-1].split('.')[0]
            with open(filePath, 'r', encoding='utf-8') as file:
                file = json.loads(file.read())
                nodes = file['data']['xdt_api__v1__clips__home__connection_v2']['edges']
                count = 0
                for node in nodes:
                    videoUrl = node['node']['media']['video_versions'][0]['url']
                    audioSavePath = f'./dataset/audio_files/{file_name_start}-{count}.mp3'
                    self.downloadAudio(videoUrl, audioSavePath)
                    with open(f'./dataset/json_files/{file_name_start}-{count}.json', 'w', encoding='utf-8') as file:
                        file.write(json.dumps(node['node'], indent=4, ensure_ascii=False))
                    count += 1
        except Exception as e:
            return f"Error processing file: {filePath}, {str(e)}"

    def downloadAudio(self, mediaUrl, audioPath): 
        try: 
            ydl_opts = {
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "192",
                    }
                ],
                "outtmpl": audioPath,
                "quiet": True,
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([mediaUrl])
            
        except Exception as e: 
            return f"Error downloading mp3"
        
if __name__=='__main__': 
    downloadReels = DownloadReels()
    redisClient = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=0)
    num_processes = cpu_count()

    with Pool(processes=num_processes) as pool:
        while True: 
            filePaths = downloadReels.getFilePaths()

            if filePaths: 
                 results = pool.map(downloadReels.download_reels, filePaths)
            else:
                print("No files to download")
                time.sleep(5)
