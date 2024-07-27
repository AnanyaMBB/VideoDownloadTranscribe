import redis
import requests 
import whisper 
import weaviate 
import weaviate.classes as wvc 

import os 
from dotenv import load_dotenv
from embedder import BertEmbedder

load_dotenv()

class TranscriptionEngine: 
    def __init__(self):
        # Load the transcription model from whisper 
        self.transcriptionModel = whisper.load_model("small.en")        

        # Setup weaviate cloud connection 
        self.headers = {"X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY")}
        self.weaviateClient = weaviate.connect_to_wcs(
            cluster_url = os.getenv("WEAVIATE_CLUSTER_URL"),
            auth_credentials = weaviate.auth.AuthApiKey(os.getenv("WEAVIATE_API_KEY")),
            headers = self.headers
        )

        # Setup Redis Client connection  
        self.redisClient = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=0)

        try:
            # If weaviate is properly connected, we proceed to other tasks 
            if self.weaviateClient.is_ready():
                print("Successfully connected to Weaviate")
            else:
                print("Weaviate is not ready")
            
        except Exception as e:
            print(f"Error connecting to Weaviate: {e}")
        finally: 
            self.weaviateClient.close()
    
    def create_schema(self): 
        try: 
            self.weaviateClient.collections.create(
                name="ReelsTranscript",
                # vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_openai(),
                vectorizer_config=wvc.config.Configure.Vectorizer.none(),
                properties=[
                    wvc.config.Property(
                        name="media_id", 
                        data_type=wvc.DataType.STRING,
                        tokenization=wvc.config.Tokenization.WORD, 
                    ),
                    wvc.config.Property(
                        name="transcription", 
                        data_type=wvc.DataType.TEXT,
                        tokenization=wvc.config.Tokenization.WORD,
                    )
                ], 
                vector_index_config=wvc.config.Configure.VectorIndex.hnsw(
                    distance_metric=wvc.config.VectorDistances.COSINE,
                    quantizer=wvc.confog.Configure.VectorIndex.Quantizer.bq(),
                ),
            )
        except Exception as e:
            print(f"Error creating schema: {e}")
        
    def add_to_weaviate(self, media_id, transcription): 
        try: 
            self.weaviateClient.collections.get("ReelsTranscript").data.insert(
                properties={
                    "media_id": media_id, 
                    "transcription": transcription
                },
                vector=""
            )
        except Exception as e:
            print(f"Error adding to Weaviate: {e}")
    
    def transcribe(self): 
        pass 

if __name__ == "__main__": 
    engine = TranscriptionEngine() 

