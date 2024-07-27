# import os
# os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"

from transformers import BertTokenizer, BertModel
import torch 

class BertEmbedder: 
    def __init__(self, model_name="bert-base-uncased"):
        self.tokenizer = BertTokenizer.from_pretrained(model_name)
        self.model = BertModel.from_pretrained(model_name)
        # self.model.eval()

    def generate_embeddings(self, text_list):
        inputs = self.tokenizer(text_list, return_tensors="pt", padding=True, truncation=True, max_length=512)
        
        with torch.no_grad(): 
            outputs = self.model(**inputs)

        embeddings = outputs.last_hidden_state

        sentence_embeddings = torch.mean(embeddings, dim=1)
        return sentence_embeddings