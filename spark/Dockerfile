FROM registry.gitlab.com/rychly-edu/docker/docker-spark:2.4.4-hadoop3.2

COPY spark/entrypoint_start.sh /
COPY docker-app/app/plaso_test /dfvfs_fs
COPY plaso_project /plaso

RUN apt update \
    && apt install -y software-properties-common \
    && add-apt-repository --yes ppa:deadsnakes/ppa \
    && add-apt-repository --yes universe \
    && add-apt-repository --yes ppa:gift/stable \
    && apt update

RUN apt install -y python3.7 \
    && apt-get install -y python3.7-dev \
    && python3.7 -m pip install --upgrade pip

RUN python3.7 -m pip install wheel

#RUN python3.7 -m pip install dfvfs

RUN python3.7 -m pip install pyarrow

WORKDIR /plaso
RUN python3.7 -m pip install "yara-python==4.0.0"
RUN python3.7 -m pip install .

WORKDIR /dfvfs_fs
RUN bash copy.sh
ENTRYPOINT ["/entrypoint_start.sh"]

HEALTHCHECK CMD /healthcheck.sh
