# storm-kafka-twitter: Scan Twitter trending topics in a distributed environment using Apache Kafka and Storm.

The goal of this project is to implement a Java application that reads tweets from both the Twitter Streaming API and preloaded log file and finds the trending topics for a given set of languages and periods. The application is able to run in a distributed environment and uses Apache Kafka to store the tweets and avoid information lost and Storm to find out the trending topics. The output of the application is a set of files, one for each language, with the most used hashtags (top-3) in between two occurrences of a given hashtag.

Instructions on how to execute
----------- 
Compile twitterApp:

````
$ cd twitterApp
$ mvn clean install
$ target/appassembler/bin/startTwitterApp.sh mode apiKey apiSecret tokenValue tokenSecret kafkaBrokerURL filename
````

Compile storm:

````
$ cd storm
$ mvn clean compile assembly:single
$ path/to/bin/storm jar target/trandingTopology-1.0-SNAPSHOT-jar-with-dependencies.jar master2016.Top3App langList kafkaBrokerURL topologyName outputFolder
````