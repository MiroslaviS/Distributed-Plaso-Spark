ARG FROM_TAG
FROM registry.gitlab.com/rychly-edu/docker/docker-spark:${FROM_TAG:-latest}

MAINTAINER Marek Rychly <marek.rychly@gmail.com>

COPY  docker-app/scripts /
COPY docker-app/app /app/
COPY plaso_project/ /plaso


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

RUN python3.7 -m pip install "yara-python==4.0.0"
RUN python3.7 -m pip install -r /plaso/requirements.txt

#RUN python3.7 -m pip install Flask pyarrow dfvfs python-libarchive \
#    && python3.7 -m pip install pyspark findspark

RUN python3.7 -m pip install Flask pyarrow python-libarchive \
    && python3.7 -m pip install pyspark findspark chardet

WORKDIR /app/plaso_test/
RUN bash copy.sh
# Plaso tools needs to be installed as last, bcs of different pytsk3 version installed by plaso packages
#RUN apt-get install -y plaso-tools

ENTRYPOINT ["/entrypoint.sh"]

#CMD ["/run.sh"]

#HEALTHCHECK CMD /healthcheck.sh
