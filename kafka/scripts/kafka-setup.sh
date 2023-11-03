# blocks until kafka is reachable
echo -e 'Creating kafka topics'
kafka-topics --bootstrap-server broker:29092 --create --topic FinnhubTrade --if-not-exists --replication-factor 1 --partition 1
echo -e 'Successfully created the following topics:'
kafka-topics --bootstrap-server localhost:29092 --list