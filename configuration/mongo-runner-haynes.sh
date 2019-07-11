## Start the Config Server 
sudo mongod --configsvr --bind_ip localhost --port 27019 --fork --replSet configServer --dbpath /data/head_node --logpath /data/head_node.log

## Connect all the config servers together (1)
mongo --port 27019 --eval 'rs.initiate({ _id: "configServer", configsvr: true, members: [ { _id : 0, host : "localhost:27019" } ] })'

## Start each shard server. This modified no longer have the --replSet flag
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node1 --port 27001 --fork --logpath /data/node1.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node2 --port 27002 --fork --logpath /data/node2.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node3 --port 27003 --fork --logpath /data/node3.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node4 --port 27004 --fork --logpath /data/node4.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node5 --port 27005 --fork --logpath /data/node5.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node6 --port 27006 --fork --logpath /data/node6.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node7 --port 27007 --fork --logpath /data/node7.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node8 --port 27008 --fork --logpath /data/node8.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node9 --port 27009 --fork --logpath /data/node9.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node10 --port 27010 --fork --logpath /data/node10.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node11 --port 27011 --fork --logpath /data/node11.log
sudo mongod --shardsvr  --bind_ip localhost --dbpath /data/mongo_node12 --port 27012 --fork --logpath /data/node12.log

## start mongos specify the query config Server
sudo mongos --configdb configServer/localhost:27019  --logappend --logpath /data/log.mongos --port 27020 --fork

## add the shards to the mongos 
mongo --port 27020 --eval 'sh.addShard("localhost:27001")'
mongo --port 27020 --eval 'sh.addShard("localhost:27002")'
mongo --port 27020 --eval 'sh.addShard("localhost:27003")'
mongo --port 27020 --eval 'sh.addShard("localhost:27004")'
mongo --port 27020 --eval 'sh.addShard("localhost:27005")'
mongo --port 27020 --eval 'sh.addShard("localhost:27006")'
mongo --port 27020 --eval 'sh.addShard("localhost:27007")'
mongo --port 27020 --eval 'sh.addShard("localhost:27008")'
mongo --port 27020 --eval 'sh.addShard("localhost:27009")'
mongo --port 27020 --eval 'sh.addShard("localhost:27010")'
mongo --port 27020 --eval 'sh.addShard("localhost:27011")'
mongo --port 27020 --eval 'sh.addShard("localhost:27012")'

## Unnecessary but should provide output that confirms that the database is sharding.
mongo --port 27020 --eval 'sh.status()'
