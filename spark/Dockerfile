FROM registry.gitlab.com/rychly-edu/docker/docker-spark:2.4.4-hadoop3.2

COPY entrypoint_start.sh /
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

RUN python3.7 -m pip install dfvfs

RUN python3.7 -m pip install pyarrow

ENTRYPOINT ["/entrypoint_start.sh"]

HEALTHCHECK CMD /healthcheck.sh
