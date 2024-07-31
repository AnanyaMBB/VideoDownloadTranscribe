import redis
import requests
import whisper
import torch
import weaviate
import weaviate.classes as wvc

import os
import json
import time
from dotenv import load_dotenv
from embedder import BertEmbedder
from multiprocessing import Pool, cpu_count
load_dotenv()


class TranscriptionEngine:
    def __init__(self):
        # Load the transcription model from whisper
        if torch.cuda.is_available():
            print("CUDA is available")
        self.transcriptionModel = whisper.load_model("small.en", device="cuda" if torch.cuda.is_available() else "cpu")

        # Setup weaviate cloud connection
        self.headers = {"X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY")}
        self.weaviateClient = weaviate.connect_to_wcs(
            cluster_url=os.getenv("WEAVIATE_CLUSTER_URL"),
            auth_credentials=weaviate.auth.AuthApiKey(os.getenv("WEAVIATE_API_KEY")),
            headers=self.headers,
        )

        # Setup Redis Client connection
        self.redisClient = redis.Redis(
            host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=0
        )

        try:
            # If weaviate is properly connected, we proceed to other tasks
            if self.weaviateClient.is_ready():
                print("Successfully connected to Weaviate")
                if not self.weaviateClient.collections.exists("ReelsTranscript"):
                    self.create_schema()
                else: 
                    print("Collection already exists")
            else:
                print("Weaviate is not ready")

        except Exception as e:
            print(f"Error connecting to Weaviate: {e}")
            self.weaviateClient.close()
        # finally:
        #     self.weaviateClient.close()
    def transcribeAndStore(self, filePath):
        self.weaviateClient = weaviate.connect_to_wcs(
            cluster_url=os.getenv("WEAVIATE_CLUSTER_URL"),
            auth_credentials=weaviate.auth.AuthApiKey(os.getenv("WEAVIATE_API_KEY")),
            headers=self.headers,
        )

        # Setup Redis Client connection
        self.redisClient = redis.Redis(
            host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=0
        )

        try:
            # If weaviate is properly connected, we proceed to other tasks
            if self.weaviateClient.is_ready():
                # print("*Successfully connected to Weaviate: Ready to write")
                transcriptionResult = self.transcribe(filePath)
                videoData = self.getVideoData(filePath)
                # print("Transcription Result: ", transcriptionResult)
                # print("Video Data: ", videoData)
                self.add_to_weaviate(transcriptionResult, videoData)
                print("Transcription and storage complete", filePath)
            else:
                print("Weaviate is not ready")

        except Exception as e:
            print(f"Error connecting to Weaviate, writing failed: {e}")
        finally:
            self.weaviateClient.close()

    def getFilePath(self):
        filePath = self.redisClient.lpop("audio_files")
        if filePath:
            filePath = filePath.decode("utf-8")
        else:
            return None
        return filePath

    def create_schema(self):
        print("Creating schema")
        try:
            self.weaviateClient.collections.create(
                name="ReelsTranscript",
                vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_openai(
                    model="text-embedding-3-large", dimensions=1024
                ),
                # vectorizer_config = [
                #     wvc.config.Configure.NamedVectors.text2vec_openai(
                #         name="caption_vector",
                #         source_properties=["caption"],
                #         model="text-embedding-3-large",
                #         dimensions=1024,
                #             vector_index_config=wvc.config.Configure.VectorIndex.hnsw(
                #             distance_metric=wvc.config.VectorDistances.COSINE,
                #             quantizer=wvc.config.Configure.VectorIndex.Quantizer.bq(),
                #         ),
                #     ),
                #     wvc.config.Configure.NamedVectors.text2vec_openai(
                #         name="transcript_vector",
                #         source_properties=["transcript"],
                #         model="text-embedding-3-large",
                #         dimensions=1024,
                #         vector_index_config=wvc.config.Configure.VectorIndex.hnsw(
                #             distance_metric=wvc.config.VectorDistances.COSINE,
                #             quantizer=wvc.config.Configure.VectorIndex.Quantizer.bq(),
                #         ),
                #     ),
                # ],
                generative_config=wvc.config.Configure.Generative.openai(),
                properties=[
                    wvc.config.Property(
                        name="media_id",
                        data_type=wvc.config.DataType.TEXT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="username",
                        data_type=wvc.config.DataType.TEXT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="likes_count",
                        data_type=wvc.config.DataType.INT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="comments_count",
                        data_type=wvc.config.DataType.INT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="audio_id",
                        data_type=wvc.config.DataType.TEXT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="caption",
                        data_type=wvc.config.DataType.TEXT,
                        tokenization=wvc.config.Tokenization.LOWERCASE,
                    ),
                    wvc.config.Property(
                        name="transcript",
                        data_type=wvc.config.DataType.TEXT,
                        tokenization=wvc.config.Tokenization.LOWERCASE,
                    ),
                ],
                vector_index_config=wvc.config.Configure.VectorIndex.hnsw(
                    distance_metric=wvc.config.VectorDistances.COSINE,
                    quantizer=wvc.config.Configure.VectorIndex.Quantizer.bq(),
                ),
                inverted_index_config=wvc.config.Configure.inverted_index(
                    index_null_state=True,
                    index_property_length=True,
                    index_timestamps=True,
                ),
            )
        except Exception as e:
            print(f"Error creating schema: {e}")

    def add_to_weaviate(
        self,
        transcription,
        video_data,
    ):
        try:
            self.weaviateClient.collections.get("ReelsTranscript").data.insert(
                properties={
                    "media_id": video_data["media_id"],
                    "username": video_data["username"],
                    "likes_count": video_data["likes_count"],
                    "comments_count": video_data["comments_count"],
                    "audio_id": video_data["audio_id"],
                    "caption": video_data["caption"],
                    "transcript": transcription,
                },
            )
        except Exception as e:
            print(f"Error adding to Weaviate: {e}")

    def transcribe(self, filePath):
        result = self.transcriptionModel.transcribe(filePath)
        return result["text"]

    def getVideoData(self, filePath):
        jsonFilePath = filePath.replace("audio_files", "json_files").replace(
            ".mp3", ".json"
        )
        with open(jsonFilePath, "r", encoding="utf-8") as file:
            data = json.load(file)
            data = data["media"]
            media_id = data["code"]
            username = data["user"]["username"]
            likes_count = data["like_count"]
            comments_count = data["comment_count"]
            audio_id = (
                data["clips_metadata"]["original_sound_info"]["audio_asset_id"]
                if data["clips_metadata"]["original_sound_info"]
                else None
            )
            caption = data["caption"]["text"]
        return {
            "media_id": media_id,
            "username": username,
            "likes_count": likes_count,
            "comments_count": comments_count,
            "audio_id": audio_id,
            "caption": caption,
        }


if __name__ == "__main__":
    engine = TranscriptionEngine()

    # num_processes = cpu_count() 
    # with Pool(processes=num_processes) as pool:
    #     while True:
    #         filePath = engine.getFilePath()
    #         print("FILE PATH", filePath)
    #         if filePath:
    #             engine.transcribeAndStore(filePath)
    #             pool.map(engine.transcribeAndStore, [filePath])

    #         else:
    #             print("No files to transcribe")
    #             time.sleep(5)

    while True:
        filePath = engine.getFilePath()
        print("FILE PATH", filePath)
        if filePath:
            engine.transcribeAndStore(filePath)
            # transcriptionResult = engine.transcribe(filePath)
            # videoData = engine.getVideoData(filePath)
            # print("Transcription Result: ", transcriptionResult)
            # print("Video Data: ", videoData)
            # engine.add_to_weaviate(transcriptionResult, videoData)

        else:
            print("No files to transcribe")
            time.sleep(5)
