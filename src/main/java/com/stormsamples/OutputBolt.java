package com.stormsamples;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * a sample bolt that outputs the incoming messages to STDOUT
 * @author roin
 * 
 */
public class OutputBolt extends BaseRichBolt
{
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger("com.stormsamples.OutputBolt");

	/**
	 * prepares the bolt
	 * 
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		logger.info(String.format("bolt is ready"));
	}

	/**
	 * this func is being called when a message is being received and it prints the message to STDOUT
	 */
	@Override
	public void execute(Tuple tuple)
	{
		if (tuple.size() >= 1)
		{
			String message = tuple.getString(0);
			logger.info(String.format("message received"));

			// output sample - output the message to STDOUT.
			System.out.println(String.format("######################\nmessage received: %s\n######################",
					message));
		}
	}

	/**
	 * declaring output fields - we have none (this is a sink).
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
	}
}
