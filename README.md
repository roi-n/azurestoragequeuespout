This sample demonstrates a simple storm topology which connects to Azure storage queue and reads messages from it.

Usage: 

```Java
public static void main(String[] args)
{
	// params
	String azureAccountName = "[Insert-Account-Name-Here]";
	String azureAccountKey = "[Insert-Account-Key-Here]";
	String azureQueueName = "[Insert-Queue-Name-Here]";
	int queueMessagesToReadAtOnce = 2;
	long queueSleepTimeBetweenDequeuesMsec = 1000;

	
	// create the topology
	IAzureStorageQueueConnection azureQueueConnection = new AzureStorageQueueConnection(String.format(
			"DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s", azureAccountName, azureAccountKey),
			azureQueueName, queueMessagesToReadAtOnce, queueSleepTimeBetweenDequeuesMsec);
	TopologyBuilder builder = new TopologyBuilder();
	builder.setSpout("azure-queue-spout", new AzureQueueSpout(azureQueueConnection), 1);
	builder.setBolt("output", new OutputBolt(), 2).shuffleGrouping("azure-queue-spout");

	// create the configuration
	Config conf = new Config();
	conf.setDebug(false);
	conf.setMaxTaskParallelism(3);

	// run local cluster
	LocalCluster cluster = new LocalCluster();
	cluster.submitTopology("topology", conf, builder.createTopology());

	// wait forever as the topology never needs to finish
	while (true)
	{
		Thread.sleep(100000);
	}
}
```

Note:
this topology uses azure storage java sdk:
https://github.com/Azure/azure-storage-java
