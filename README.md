# Distributed Plaso Spark (Plasospark)
A tool for distributed extraction of events from various files using extractors adapted from Plaso engine to Apache Spark.

# Usage
The Plasospark is running in docker container and is accessable via Rest API.
For usage see [User Guide](https://github.com/MiroslaviS/Distributed-Plaso-Spark/wiki/User-Guide) wiki page.

# Deployment

## Use pre-built images 
There is prebuilt image for Spark services 
There is prebuilt image for Webservice Plasospark

## Build
For instruction how to build the tool images see [Installation guide](https://github.com/MiroslaviS/Distributed-Plaso-Spark/wiki/Installation-guide).
And also see webservice [Dockerfile](https://github.com/MiroslaviS/Distributed-Plaso-Spark/blob/master/docker-app/Dockerfile) and Spark [Dockerfile](https://github.com/MiroslaviS/Distributed-Plaso-Spark/blob/master/spark/Dockerfile).

## Results
Testing results can be seen in wiki page [Test Results](https://github.com/MiroslaviS/Distributed-Plaso-Spark/wiki/Plasospark-test-results).


## Dependencies
The Plasospark is using the following projects:
* utilizing extractors and internal componenets from the [Plaso project](https://github.com/log2timeline/plaso)
* Using docker images from [TARZAN Docker Infrastructure Project](https://gitlab.com/rychly-edu/projects/tarzan-docker-infrastructure)
  * Original Hadoop [HDFS image](https://gitlab.com/rychly-edu/docker/docker-hdfs)
  * Customized Apache [Spark image](https://gitlab.com/rychly-edu/docker/docker-spark)
