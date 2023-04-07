FROM python:3.9
WORKDIR ./
COPY ./requirement.txt ./requirement.txt
RUN pip install -r requirement.txt
COPY ./container_with_envs_v2.py ./container_with_envs_v2.py
CMD ["python3","./container_with_envs_v2.py"]
