#  Elasticsearch Indexer

Example index creation and mock data loading to reproduce "None of the configured nodes were available" problem when indexing to Elastic Cloud when running code on AWS.

1. Clone the repository
2. Run gradle fatJar
3. In elasticsearch-indexer/build/libs java -jar elasticsearch-indexer-all-1.0-SNAPSHOT.jar "clusterId" "region" "username" "password" 3 10000 1000

The cluster and the indexing code is running in the region: us-east-1. When I run the client in a different region to the cluster I have not been able to reproduce the problem

AWS Redhat AMI:

	java version "1.8.0_112"
	Java(TM) SE Runtime Environment (build 1.8.0_112-b15)
	Java HotSpot(TM) 64-Bit Server VM (build 25.112-b15, mixed mode)
	
	Example results
	
	java -jar elasticsearch-indexer-all-1.0-SNAPSHOT.jar <clusterid> us-east-1 admin "password" 3 10000 1000
    18:52:40,575  INFO LoaderApplication:235 - Connecting client to: 107.21.225.225
    18:52:42,193  INFO LoaderApplication:86 - Waiting for index health to be green
    18:52:42,318 DEBUG LoaderApplication:143 - Sending: 1000 records, in execution: 1, to: 107.21.225.225
    18:52:42,509  INFO LoaderApplication:161 - Bulk Load Failed : 1, address: 107.21.225.225, message: None of the configured nodes were available: [{instance-0000000009}{rpUJ}{10.35.205.168}{107.21.225.225:9343}{logical_availability_zone=zone-0, availability_zone=us-east-1e, max_local_storage_nodes=1, region=us-east-1, master=true}]
    18:52:42,535 DEBUG LoaderApplication:143 - Sending: 1000 records, in execution: 2, to: 107.21.225.225
    18:52:42,592  INFO LoaderApplication:161 - Bulk Load Failed : 10, address: 107.21.225.225, message: None of the configured nodes were available: [{instance-0000000009}{rpUJ}{10.35.205.168}{107.21.225.225:9343}{logical_availability_zone=zone-0, availability_zone=us-east-1e, max_local_storage_nodes=1, region=us-east-1, master=true}]
    
    java -jar elasticsearch-indexer-all-1.0-SNAPSHOT.jar <clusterid> us-east-1 admin "password" 3 10000 1000
    18:52:48,025  INFO LoaderApplication:235 - Connecting client to: 54.235.223.164
    18:52:49,659  INFO LoaderApplication:86 - Waiting for index health to be green
    18:52:49,804 DEBUG LoaderApplication:143 - Sending: 1000 records, in execution: 1, to: 54.235.223.164
    18:52:50,086  INFO LoaderApplication:153 - Successful execution request: 1, address: 54.235.223.164
    18:52:50,696 DEBUG LoaderApplication:143 - Sending: 1000 records, in execution: 10, to: 54.235.223.164
    18:52:50,787  INFO LoaderApplication:153 - Successful execution request: 10, address: 54.235.223.164