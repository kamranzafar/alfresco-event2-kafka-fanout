# ACS Community repository image with this module's AMP installed.
# Build from the project root after `mvn package`:  docker build -f docker/Dockerfile .
ARG ACS_IMAGE=alfresco/alfresco-content-repository-community:26.2.0
FROM ${ACS_IMAGE}

USER root
COPY target/alfresco-event2-kafka-fanout-*.amp /usr/local/tomcat/amps/
RUN java -jar /usr/local/tomcat/alfresco-mmt/alfresco-mmt*.jar install \
        /usr/local/tomcat/amps/alfresco-event2-kafka-fanout-*.amp /usr/local/tomcat/webapps/alfresco -nobackup -force \
    && java -jar /usr/local/tomcat/alfresco-mmt/alfresco-mmt*.jar list /usr/local/tomcat/webapps/alfresco
USER alfresco
