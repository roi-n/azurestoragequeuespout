package com.stormsamples;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * an azure storage queue spout
 * @author roin
 * 
 */
public class AzureQueueSpout extends BaseRichSpout
{
	private static final long serialVersionUID = 2710625194299149138L;
	private SpoutOutputCollector outputCollector;
	private IAzureStorageQueueConnection queueConnection;
	private static final Logger logger = Logger.getLogger("com.stormsamples.AzureStorageQueueSpout");
	
	/**
	 * Ctor
	 * @param azureStorageQueueConnection: the azure queue connection
	 */
	public AzureQueueSpout(IAzureStorageQueueConnection azureStorageQueueConnection)
	{
		this.queueConnection = azureStorageQueueConnection;
		logger.info("spout created");
	}

	/**
	 * opens the spout (connects to azure queue)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
	{
		// save the collector
		outputCollector = collector;
		// and connect to the storage queue
		queueConnection.connect();
		logger.info(String.format("opened spout and connecetd to azure queue: %s. success: %b", queueConnection.getQueueName(), queueConnection.isConnected()));
	}

	/**
	 * this is being called when a new tuple is ready to be emitted
	 */
	@Override
	public void nextTuple()
	{
		// if the queue is connected or a new connection try succeeds, get messages from the queue
		if ((queueConnection.isConnected()) || (queueConnection.connect()))
		{
			List<String> messages = queueConnection.getMessages();
			if (messages != null)
			{
				for (String message : messages)
				{
					outputCollector.emit(new Values(message));
					logger.info(String.format("emitting message: %s", message));
				}
			}
		}
	}

	/**
	 * declares the output fields. we have a single field - the queue message 
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("message"));
	}
}
