from weaviate.classes.config import Configure, Property, DataType, Tokenization
from weaviate.classes.init import Auth
import logging
import os
import pandas as pd
import pickle
import rispy
import weaviate
import uuid

# from tools.unstructure_pdf import unstructure_pdf
from tools.text_to_weaviate import merge_pickle_list, fix_utf8, split_chunks

logging.basicConfig(
    filename="weaviate.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)


http_host = os.environ["WEAVIATE_HTTP_HOST"]
http_port = os.environ["WEAVIATE_HTTP_PORT"]
grpc_host = os.environ["WEAVIATE_GRPC_HOST"]
grpc_port = os.environ["WEAVIATE_GRPC_PORT"]
weaviate_api_key = os.environ["WEAVIATE_API_KEY"]

client = weaviate.connect_to_custom(
    http_host=http_host,  # Hostname for the HTTP API connection
    http_port=http_port,  # Default is 80, WCD uses 443
    http_secure=False,  # Whether to use https (secure) for the HTTP API connection
    grpc_host=grpc_host,  # Hostname for the gRPC API connection
    grpc_port=grpc_port,  # Default is 50051, WCD uses 443
    grpc_secure=False,  # Whether to use a secure channel for the gRPC API connection
    # auth_credentials=Auth.api_key(weaviate_api_key),  # API key for authentication
)
client.collections.delete("Audit")

if not client.collections.exists(name="Audit"):
    collection = client.collections.create(
        name="Audit",
        properties=[
            Property(
                name="content",
                data_type=DataType.TEXT,
                vectorize_property_name=True,
                tokenization=Tokenization.GSE,
            ),
            Property(
                name="source",
                data_type=DataType.TEXT,
                tokenization=Tokenization.GSE,
            ),
            Property(
                name="doc_chunk_id",
                data_type=DataType.TEXT,
            )
        ],
        vectorizer_config=[
            Configure.NamedVectors.text2vec_transformers(
                name="content", source_properties=["content"]
            ),
        ],
    )
else:
    collection = client.collections.get(name="Audit")


path = "law/test.ris"

# 设置读取ris文件的根目录
root = path.rsplit("/", 1)[0] + "/"

# 读取ris文件
with open(path, "r") as bibliography_file:
    records = rispy.load(bibliography_file)
    filtered_records = []
    for record in records:
        authors = ", ".join(record.get("authors", []))
        filtered_record = {
            "title": record.get("title", ""),
            "author": authors,
            "date_enacted": record.get("date", ""),
            "file_attachments1": record.get("file_attachments1", ""),
        }
        filtered_records.append(filtered_record)

df_ris = pd.DataFrame(filtered_records)


for index, row in df_ris.iterrows():
    # try:
        path = root + row["file_attachments1"]
        source = f"{row['title']}, {row['author']}, {row['date_enacted']}"
        path_without_pdf = path.replace(".pdf", "")
        pickle_path = f"{path_without_pdf}.pkl"

        # # # # 如果已有pickle文件则跳过
        # if os.path.exists(pickle_path):
        #     continue

        # # # 如果没有则进行处理
        # text_list = unstructure_pdf(path)

        # # # 存pickle        
        # with open(pickle_path, "wb") as f:
        #     pickle.dump(text_list, f)
        # logging.info(f"created {pickle_path}")
        
        # # 读pickle
        with open(pickle_path, "rb") as f:
            text_list = pickle.load(f)
        data = merge_pickle_list(text_list)
        data = fix_utf8(data)
        id = str(uuid.uuid4())
        chunks = split_chunks(id=id, text_list=data, source=source)

        collection.data.insert_many(chunks)
        logging.info(f"Inserted {path}")
    # except Exception as e:
    #     logging.error(f"Error processing {path}: {e}")

client.close()
