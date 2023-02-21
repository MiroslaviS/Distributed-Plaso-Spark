ARG FROM_TAG
FROM registry.gitlab.com/rychly-edu/docker/docker-spark:${FROM_TAG:-latest}

MAINTAINER Marek Rychly <marek.rychly@gmail.com>

COPY scripts /
COPY app /app/

ENV  \
SPARK_JARS=/app/lib \
SPARK_PYFILES=/app/lib \
SPARK_APP=/app/main.py

RUN true && chmod 755 /*.sh \
    && apt update \
    && apt install -y software-properties-common \
    && add-apt-repository --yes ppa:deadsnakes/ppa \
    && add-apt-repository --yes universe \
    && add-apt-repository --yes ppa:gift/stable \
    && apt update \
    && apt install -y python3.7 \
    && apt-get install -y python3.7-dev \
    && apt-get install plaso-tools \
    && python3.7 -m pip install --upgrade pip \
    && python3.7 -m pip install Flask pyarrow dfvfs python-libarchive


ENTRYPOINT ["/entrypoint.sh"]

#CMD ["/run.sh"]

#HEALTHCHECK CMD /healthcheck.sh
