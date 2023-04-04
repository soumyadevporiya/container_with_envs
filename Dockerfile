FROM python:3.9
WORKDIR ./
COPY ./requirement.txt ./requirement.txt
RUN pip install -r requirement.txt
COPY ./bq_storage_read_api.py ./bq_storage_read_api.py
CMD ["python3","./bq_storage_read_api.py"]
