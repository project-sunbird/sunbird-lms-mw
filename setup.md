## Pre-requisites
1. Cassandra
    1. Install Cassandra database and start the server
    2. Run [cassandra.cql](https://github.com/project-sunbird/sunbird-lms-mw/blob/master/actors/src/main/resources/cassandra.cql) file to create the required keyspaces, tables and indices
2. ElasticSearch
    1. Install ElasticSearch database and start the server

## Configuration
1. Environment Variables:
    1. sunbird_cassandra_host: host running the cassandra server
    2. sunbird_cassandra_port: port on which cassandra server is running
    3. sunbird_cassandra_username (optional): username for cassandra database, if authentication is enabled
    4. sunbird_cassandra_password (optional): password for cassandra database, if authentication is enabled
    5. sunbird_es_host: host running the elasticsearch server
    6. sunbird_es_port: port on which elasticsearch server is running
    7. sunbird_es_cluster (optional): name of the elasticsearch cluster
    8. sunbird_actor_file_path: Actor configuration is provided via [application.conf](https://github.com/project-sunbird/sunbird-lms-mw/blob/master/actors/src/main/resources/application.conf) file. The project is bundled with default application.conf file which runs 5 instances of each actor with hostname as "127.0.0.1" and on the port "8088". The following configuration can be overrided by providing a custom application.conf file:
        1. hostname: the hostname on the which the akka actors will be listening
        2. port: port on which the akka actors will be listening
        3. router: type of router to be used for switching between actors
        4. nr-of-instances: number of instances of actor to run in this host

## Build
1. Run "git submodule foreach git pull origin master" to pull the latest sunbird-common submodule.
2. Run "mvn clean install" from "sunbird-lms-mw/actors" to build the actors.
3. The build file is a executable jar file "learner-actor-1.0-SNAPSHOT.jar" generated in "sunbird-lms-mw/actors/target" folder

## Run
1. Actors can be started by running **java -cp "learner-actor-1.0-SNAPSHOT.jar" org.sunbird.learner.Application**
