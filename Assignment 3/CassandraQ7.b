Question7: 

cs6360:~} /usr/local/apache-cassandra-2.0.5/bin/nodetool -h csac0 status

Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address       Load       Tokens  Owns   Host ID                               Rack
UN  10.176.92.91  213.42 MB  256     25.0%  f402386c-f200-489c-ba35-d37b2b880b82  rack1
UN  10.176.92.92  194.44 MB  256     26.3%  29838b5d-1523-43fe-b6ff-e357a8995861  rack1
DN  10.176.92.93  228.19 MB  256     25.4%  67080a87-dbf3-4fe2-84a0-d3ad4a9d6585  rack1
UN  10.176.92.94  203.48 MB  256     23.4%  d4b9544b-37ef-4421-ab65-026eb889e37d  rack1

The cluster is equally balanced.