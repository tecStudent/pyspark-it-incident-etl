FROM spark:python3@sha256:469921a4c3dbb534b17fa51ff4da61fa1fda3c27f0882fa7809b63c2c2bc2d8d

USER root

COPY requirements.txt /tmp/requirements.txt

RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

USER 185