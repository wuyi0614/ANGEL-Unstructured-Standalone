
# The Unstructured Standalone instance for ANGEL-NTU

**Acknowledgement**: this project was derived from [Tiangong-AI Unstructure Local](https://github.com/linancn/TianGong-AI-Unstructure-Local). 

## Quick start 

**First**, get dependencies and Python interpreter ready for deployment:

```bash
# use Python 3.12 interpreter and pipenv to manage the dependencies
pipenv install 
```

**Second**, ensure your docker engine is ready and use the correct `docker-compose.yml` to launch your service.

- If you're not blocked by docker-hub, use `docker-compose-*-noblock.yml` to launch the service
- If you're blocked, download images from docker-hub's mirror before executing `docker-compose`

Downloading the images by using the mirror as the prefix: `m.daocloud.io/`,

```bash
# use mirrored images from m.daocloud.io
docker pull m.daocloud.io/cr.weaviate.io/semitechnologies/weaviate:1.28.4
docker pull m.daocloud.io/cr.weaviate.io/semitechnologies/transformers-inference:sentence-transformers-paraphrase-multilingual-MiniLM-L12-v2

# in this case, the names of images are changed, and move to the root dir before docker-compose up
docker-compose -f docker/docker-compose-cpu.yml up

```
