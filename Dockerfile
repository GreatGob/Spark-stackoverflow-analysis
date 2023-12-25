FROM mongo:6.0.4-jammy

USER root

###############################
## Install MongoDB Tools
###############################
RUN apt-get update && apt-get install -y curl

RUN cd "/tmp" \
    && curl -o "mongodb-database-tools-ubuntu1604-x86_64-100.8.0.deb" "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu1604-x86_64-100.8.0.deb" \
    &&  apt-get install -y "./mongodb-database-tools-ubuntu1604-x86_64-100.8.0.deb" \
    && rm "./mongodb-database-tools-ubuntu1604-x86_64-100.8.0.deb"