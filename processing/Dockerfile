FROM python:3.8-alpine
LABEL maintainer="Joshua Samuel"

ENV PYTHONPATH=/usr/lib/python3.8/site-packages
RUN echo 'https://dl-cdn.alpinelinux.org/alpine/edge/community' >> /etc/apk/repositories && \
    apk --no-cache add py3-numpy py3-pandas py3-regex && \
    pip install cantools aws-xray-sdk boto3 pandas six regex --no-cache 
ADD . /process/
WORKDIR /process/

ENTRYPOINT python3 /process/canmsgtosignals.py
