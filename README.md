# storm-kafka-twitter: Scan Twitter trending topics in a distributed environment using Apache Kafka and Storm.

The goal of this project is to implement a Java application that reads tweets from both the Twitter Streaming API and preloaded log file and finds the trending topics for a given set of languages and periods. 

The application is able to run in a distributed environment and uses Apache Kafka to store the tweets and avoid information lost and Storm to find out the trending topics. The output of the application is a set of files, one for each language, with the most used hashtags (top-3) in between two occurrences of a given hashtag.

Installation
----------- 
Dependencies:

* Oracle Java 7
* Storm 1.0.2
* Kafka 2.11-0.10.1.0

Instructions on how to execute
----------- 
The TwitterApp has been configured and compiled with the ````appassembler```` maven plugin. Furthermore, a [Twitter development account](https://apps.twitter.com/) is needed in order to consume streaming live tweets:

````
$ cd twitterApp
$ mvn clean install
$ target/appassembler/bin/startTwitterApp.sh mode apiKey apiSecret tokenValue tokenSecret kafkaBrokerURL filename
````

where:

* ````mode````: 1 means read from file, 2 read from the Twitter API.
* ````apiKey````: key associated with the Twitter app consumer.
* ````apiSecret````: secret associated with the Twitter app consumer.
* ````tokenValue````: access token associated with the Twitter app.
* ````tokenSecret````: access token secret.
* ````kafkaBrokerURL````: String in the format IP:port corresponding with the Kafka Broker
* ````filename````: path to the file with the tweets.

Compile and execute Storm Topology:

````
$ cd storm
$ mvn clean compile assembly:single
$ path/to/bin/storm jar target/trandingTopology-1.0-SNAPSHOT-jar-with-dependencies.jar master2016.Top3App langList kafkaBrokerURL topologyName outputFolder
````

where:

* ````langList````: String with the list of languages (“lang” values) we are interested in and the associated special token. The list is in CSV format,
example: en:house,pl:universidade,ar:carro,es:ordenador.
* ````kafkaBrokerURL````: String IP:port of the Kafka Broker.
* ````topologyName````: String identifying the topology in the Storm Cluster.
* ````outputFolder````: path to the folder used to store the output files.

TwitterApp
-----------
 Java application that reads tweets from	both the Twitter Streaming API and preloaded log file. The application uses Apache Kafka to store information in order to be able to run the application in a distributed environment. Furthermore, Kadka is horizontally scalable, fault-tolerant and avoid information lost.

 Storm Topology
 -----------
 Storm topology for calculating the three more common hashtags (top3) in a set of languages over a configurable period of time (conditional window). A conditional window keeps all the tuples between two occurrences of a given word in the same language.