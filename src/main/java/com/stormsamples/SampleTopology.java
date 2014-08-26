package com.stormsamples;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * 
 * This topology demonstrates azure storage queue spout usage
 * 
 * @author roin
 */
public class SampleTopology
{
	private static final Logger logger = Logger.getLogger("com.stormsamples.SampleTopology");
	
	public static void main(String[] args) throws Exception
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
		logger.info("submitting local topology");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("topology", conf, builder.createTopology());

		// wait forever as the topology never needs to finish
		while (true)
		{
			Thread.sleep(100000);
		}
	}
}
