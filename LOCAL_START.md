To test replication locally:

1) `mw clean package -DskipTests`
2) `./run-built-activemq.sh start`
3) open [first broker console](http://127.0.0.1:8161) and [second broker console](http://127.0.0.1:8162)
4) log in (`admin`/`admin`)
5) create a queue with the same name in both *(this step will get gone when CRUD replication is implemented)*
6) send a message to the queue to first broker.
7) make sure it's appeared in second broker