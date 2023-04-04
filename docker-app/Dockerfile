ARG FROM_TAG
FROM registry.gitlab.com/rychly-edu/docker/docker-spark:${FROM_TAG:-latest}

MAINTAINER Marek Rychly <marek.rychly@gmail.com>

# Copy entrypoint, run, healthcheck and helpes script for spark env setup
COPY  docker-app/scripts /

# Copy sparkapp source codes to docker
COPY docker-app/app /app/

# Copy plaso source codes for sparkapp-plaso dependencies
COPY plaso_project/ /plaso/


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
    && apt update

RUN apt install -y python3.7 \
    && apt-get install -y python3.7-dev \
    && python3.7 -m pip install --upgrade pip

# Yara-python needs to be installed outside the requirements, problem with latest 4.4.x version (3.4.2023)
RUN python3.7 -m pip install "yara-python==4.0.0"

# Install plaso requirements
WORKDIR /plaso
RUN python3.7 -m pip install .
RUN #python3.7 -m pip install -r /plaso/requirements.txt

# install sparkapp libraries for running API, Spark and HDFS
RUN python3.7 -m pip install Flask pyarrow python-libarchive \
    && python3.7 -m pip install pyspark findspark chardet

# Run copy script to copy Hadoop DFVFS new filesystem/file entry/file io/path specification classes
# into DFVFS package to be able to run with plaso tools (pinfo, psort...)
WORKDIR /app/dfvfshadoop/
RUN bash copy.sh

ENTRYPOINT ["/entrypoint.sh"]
