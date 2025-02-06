import logging
import os
import pandas as pd
import pickle
import rispy
import requests


logging.basicConfig(
    filename="weaviate.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)

pdf_url = "http://thuenv.tiangong.world:7770/pdf"
docx_url = "http://localhost:8770/docx"
ppt_url = "http://localhost:8770/ppt"
token = os.environ["BEARER_TOKEN"]


def unstructure_by_service(doc_path, url, token, pickle_path):
    with open(doc_path, "rb") as f:
        files = {"file": f}
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(url, files=files, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        result = response_data.get("result")

        with open(pickle_path, "wb") as pkl_file:
            pickle.dump(result, pkl_file)


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
    filename_with_ext = os.path.basename(path)
    dir_part = os.path.dirname(path)
    basename_without_ext = os.path.splitext(filename_with_ext)[0]
    pickle_path = f"{os.path.join(dir_part, basename_without_ext)}.pkl"

    # # # 如果已有pickle文件则跳过
    if os.path.exists(pickle_path):
        continue

    # 如果没有则进行处理
    if filename_with_ext.endswith(".pdf"):
        unstructure_by_service(path, pdf_url, token, pickle_path)
    elif filename_with_ext.endswith(".docx"):
        unstructure_by_service(path, docx_url, token, pickle_path)
    elif filename_with_ext.endswith(".pptx"):
        unstructure_by_service(path, ppt_url, token, pickle_path)
