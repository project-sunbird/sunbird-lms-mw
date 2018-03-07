FROM openjdk:8-jdk-alpine
MAINTAINER "Manojv" "manojv@ilimi.in"
RUN apk update \
    && apk add unzip \
    && apk add curl \
    && adduser -u 1001 -h /home/sunbird/ -D sunbird \
    && mkdir -p /home/sunbird/learner
COPY ./service/target/actor-service-1.0-SNAPSHOT.jar /home/sunbird/learner/
RUN chown -R sunbird:sunbird /home/sunbird
EXPOSE 8088
USER sunbird
WORKDIR /home/sunbird/learner/
RUN mkdir -p /home/sunbird/learner/logs/
RUN touch /home/sunbird/learner/logs/learnerActorProject.log
RUN ln -sf /dev/stdout /home/sunbird/learner/logs/learnerActorProject.log
CMD ["java",  "-cp", "actor-service-1.0-SNAPSHOT.jar", "-Dactor_hostname=actor-service", "-Dbind_hostname=0.0.0.0", "org.sunbird.learner.Application"]
