## Pre-requisites
1. Cassandra
    1. Install Cassandra database and start the server
    2. Run [cassandra.cql](https://github.com/project-sunbird/sunbird-lms-mw/blob/master/actors/src/main/resources/cassandra.cql) file to create the required keyspaces, tables and indices
    3. Copy pageMgmt.csv and pageSection.csv to a temp folder on cassandra machine. e.g.: /tmp/cql/pageMgmt.csv and /tmp/cql/pageSection.csv.
    4. Execute the command: cqlsh -e "COPY sunbird.page_management(id, appmap,createdby ,createddate ,name ,organisationid ,portalmap ,updatedby ,updateddate ) FROM '/tmp/cql/pageMgmt.csv'"
    5. Execute the command: cqlsh -e "COPY sunbird.page_section(id, alt,createdby ,createddate ,description ,display ,imgurl ,name,searchquery , sectiondatatype ,status , updatedby ,updateddate) FROM '/tmp/cql/pageSection.csv'"
	
2. ElasticSearch
    1. Install ElasticSearch database and start the server
	2. Run this curl command
	curl -X PUT \
	  http://localhost:9200/sunbird/org/ORG_001 \
	  -H 'cache-control: no-cache' \
	  -H 'content-type: application/json' \
	  -H 'postman-token: caa7eaa7-2a08-d1f3-1eb2-bf7c73bef663' \
	  -d '{}'

## Configuration
1. Environment Variables:
    1. sunbird_cassandra_host: host running the cassandra server
    2. sunbird_cassandra_port: port on which cassandra server is running
    3. sunbird_cassandra_username (optional): username for cassandra database, if authentication is enabled
    4. sunbird_cassandra_password (optional): password for cassandra database, if authentication is enabled
    5. sunbird_es_host: host running the elasticsearch server
    6. sunbird_es_port: port on which elasticsearch server is running
    7. sunbird_es_cluster (optional): name of the elasticsearch cluster
    8. sunbird_learner_actor_host: host running for learner actor
    9. sunbird_learner_actor_port: port on which learner actor is running.
    10. sunbird_sso_url: url for keycloak server
    11. sunbird_sso_realm: keycloak realm name
    12. sunbird_sso_username: keycloak user name
    13. sunbird_sso_password: keycloak password
    14. sunbird_sso_client_id: key cloak client id
    15. sunbird_sso_client_secret : keycloak client secret (not mandatory)
    16. ekstep_content_search_base_url : provide base url for EkStep content search
    17. ekstep_authorization : provide authorization for value for content search
    18. sunbird_pg_host: postgres host name or ip
    19. sunbird_pg_port: postgres port number
    20. sunbird_pg_db: postgres db name
    21. sunbird_pg_user: postgres db user name
    22. sunbird_pg_password: postgress db password 
    23. sunbird_installation
    24. ekstep_api_base_url
    25. sunbird_mail_server_host
    26. sunbird_mail_server_port
    27. sunbird_mail_server_username
    28. sunbird_mail_server_password
    29. sunbird_mail_server_from_email
    30. sunbird_account_name : account name for azure file upload container.
    31. sunbird_account_key : azure account key

## Build
1. Clone the repository "sunbird-lms-mw"
1. Run "git submodule foreach git pull origin master" to pull the latest sunbird-common submodule.
2. Run "mvn clean install" to build the actors.
3. The build file is a executable jar file "learner-actor-1.0-SNAPSHOT.jar" generated in "sunbird-lms-mw/actors/target" folder

## Run
1. Actors can be started by running **java -cp "learner-actor-1.0-SNAPSHOT.jar" org.sunbird.learner.Application**	
	
