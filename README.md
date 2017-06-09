# SVP-MS

An implementation of XML Simple Virtual Partitioning with a master-slave load balance mechanism. 

# Executing SVP-MS

1 - Generate XML dataset catalog

2 - Edit files input/PartixVPConfig.xml and input/CQP.conf to configure host:port of the processing nodes that will be used in the query processing

3 - Run NodeQueryProcessorEngine.java on each processing node. This makes the nodes listen and wait for subqueries to process. The parameters are as follows:

- String hostName, int port, String databaseName, String dbLogin, String dbPassword, boolean quotedDateIntervals, int dbmsX

Examples of possible values:

- java NodeQueryProcessorEngine 192.168.0.11 3001 dblp SYSTEM MANAGER 0 1
- java NodeQueryProcessorEngine localhost 3002 dblp SYSTEM MANAGER 0 1

The dbmsx parameter should be set ot 1 if you are using Sedna on each node, and 2 for BaseX

4 - Run CQP_Scheduler_Engine.java on the coordinator node. It keeps waiting for a client query to be processed. The parameters are as follows:

- String CQP_address, int CQP_port_number, String ConfigFileName

Examples of possible values are as follows:

- java CQP_Scheduler_Engine 192.168.0.11 8050 "input/CQP.conf"

5 - Run ClusterQueryProcessorAdmin.java. The parameters are as follows:

- String ServerName, int PortNumber, STRING op

The op parameter can be set to SHUTDOWN or SET_CLUSTER_SIZE size

Example:

- java ClusterQueryProcessorAdmin 192.168.0.11 8050 SET_CLUSTER_SIZE 2

6 - Run ConnectionManagerImpl.java. The parameters are as follows:

- int port, String configFileName

Example:

- java ConnectionManagerImpl 8051 "input/PartiXVPConfig.xml"

7 - Run TestAvpLoadBalancing.java. The parameters are as follows:

- String cqp_address, int port, int numNQPs, String vpStrategy, int idQuery, int numberOfExecutions, boolean performDynamicLoadBalancing(true|false)

The options for the vpStrategy parameter are SVP, AVP, AVP_WR, CENTRALIZED

Example: 

- java TestVirtualPartitioning 192.168.0.11 8050 2 AVP 1 1 false

8 - Run TestAvpDummy.java. The parameters are as follows:

- String node_address, int port, int numNQPs, String vpStrategy, int idQuery, int numberOfExecutions, boolean performDynamicLoadBalancing

Example:

- java TestAvpDummy localhost 8050 2 AVP 1 1 false

# License Terms

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
