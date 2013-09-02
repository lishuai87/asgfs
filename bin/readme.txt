how to run the program: java -jar xxx.jar

file:
1. master.jar
master node, must start first, then it will print the server IP.

2. chunkserver.jar
chunkserver node, input master IP to register in master node.

3. client.jar
client program, it can upload, download and append file.


please put the test data into data/client
