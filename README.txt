PASSO A PASSO DA EXECUÇÃO
0 - Gerar catálogo da base de dados

1 - Editar (input/PartixVPConfig.xml e) input/CQP.conf com os dados dos nós de processamento
host:porta

2 - Rodar NodeQueryProcessorEngine.java em cada nó de processamento. Fica executando aguardando as subconsultas com os intervalos
String hostName, int port, String databaseName, String dbLogin, String dbPassword, boolean quotedDateIntervals, int dbmsX

192.168.0.11 3001 dblp SYSTEM MANAGER 0 1
localhost 3002 dblp SYSTEM MANAGER 0 1

dbmsx = 1 Sedna, 2 BaseX

2 - Rodar CQP_Scheduler_Engine.java no nó coordenador. Fica executando aguardando a consulta do cliente.
java CQP_Scheduler_Engine CQP_address CQP_port_number ConfigFileName
192.168.0.11 8050 "input/CQP.conf"

3 - Rodar ClusterQueryProcessorAdmin.java
java ClusterQueryProcessorAdmin ServerName PortNumber op //op = SHUTDOWN | SET_CLUSTER_SIZE size
192.168.0.11 8050 SET_CLUSTER_SIZE 2

4 - Rodar ConnectionManagerImpl.java
int port, String configFileName
8051 "input/PartiXVPConfig.xml"

5 - Rodar TestAvpLoadBalancing
java TestVirtualPartitioning cqp_address port numNQPs vpStrategy idQuery numberOfExecutions performDynamicLoadBalancing(true|false)
192.168.0.11 8050 2 SVP|AVP|AVP_WR|CENTRALIZED 1 1 false


java TestAvpDummy node_address port numNQPs vpStrategy idQuery numberOfExecutions performDynamicLoadBalancing(true|false)
localhost 8050 2 SVP|AVP|AVP_WR|CENTRALIZED 1 1 false