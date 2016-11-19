#  Elasticsearch Indexer

Example index creation and mock data loading to reproduce "None of the configured nodes were available" problem when indexing to Elastic Cloud when running code on AWS.

1. Clone the repository
2. Run gradle fatLar
3. In elasticsearch-indexer/build/libs java -jar elasticsearch-indexer-all-1.0-SNAPSHOT.jar <clusterId> <region> <username> <password> 3 10000 1000


AWS Redhat AMI:

java version "1.8.0_112"
Java(TM) SE Runtime Environment (build 1.8.0_112-b15)
Java HotSpot(TM) 64-Bit Server VM (build 25.112-b15, mixed mode)