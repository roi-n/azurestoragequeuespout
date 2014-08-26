package com.stormsamples;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * 
 * this class implements a connection to azure storage queue. it connects to the queue and can read messages from it
 * @author roin
 */
public class AzureStorageQueueConnection implements IAzureStorageQueueConnection, Serializable
{

	private static final long serialVersionUID = -3162436862662962401L;

	private String connectionString;
	private String queueName;
	private Long messageCount;
	private CloudQueue queue;
	private Boolean isConnected = false;
	private Integer numOfMessagesToRead = 1;
	private Long sleepBetweenDequeuesMsec = 1000L;
	private int messageInvisiblePeriodSec = 300;

	private static final Logger logger = Logger.getLogger("com.stormsamples.AzureStorageQueueConnection");

	/**
	 * Ctor
	 * 
	 * @param connectionString : the connection string to azure storage
	 * @param queueName : azure queue name
	 * @param numOfMessagesToRead : the number of messages to read every access to the queues (max is 32)
	 * @param sleepBetweenDequeuesMsec : Milliseconds to sleep between two consecutive dequeues (in order to let
	 *            messages accumulate, and get several messages using one call)
	 */
	public AzureStorageQueueConnection(String connectionString, String queueName, Integer numOfMessagesToRead,
			Long sleepBetweenDequeuesMsec)
	{
		this.connectionString = connectionString;
		this.queueName = queueName;

		if ((numOfMessagesToRead > 0) && (numOfMessagesToRead <= 32))
		{
			this.numOfMessagesToRead = numOfMessagesToRead;
		}
		if (sleepBetweenDequeuesMsec > 0)
		{
			this.sleepBetweenDequeuesMsec = sleepBetweenDequeuesMsec;
		}
		logger.info(String
				.format("created azure storage queue connection: connectionString: %s queueName: %s, numOfMessagesToRead: %d, sleepBetweenDequeuesMsec: %d",
						this.connectionString, this.queueName, this.numOfMessagesToRead, this.sleepBetweenDequeuesMsec));
	}

	/**
	 * returns the queue connection string
	 */
	public String getConnectionString()
	{
		return this.connectionString;
	}

	/**
	 * returns the queue name
	 */
	public String getQueueName()
	{
		return this.queueName;
	}

	/**
	 * returns the estimated messages count from the queue
	 */
	public Long getApproximateMessageCount()
	{
		try
		{
			if (queue != null)
			{
				// try to get the approximated message count from the queue
				queue.downloadAttributes();
				messageCount = queue.getApproximateMessageCount();
				return messageCount;
			}
		}
		catch (Exception ex)
		{
			logger.error(String.format("error getting message count from azure queue (%s): error data: ", queueName,
					ex.getMessage()));
		}
		// no queue or error getting the data from it
		return 0L;
	}

	/**
	 * returns true if the connection to the queue was successful or false otherwise
	 */
	public boolean isConnected()
	{
		return this.isConnected;
	}

	/**
	 * connects to the queue (sets isConnected to true on success or false otherwise)
	 */
	public boolean connect()
	{
		try
		{
			isConnected = false;
			if ((connectionString != null) && (connectionString.length() > 0) && (queueName != null)
					&& (queueName.length() > 0))
			{
				// connect to the storage account and create queue client
				CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
				CloudQueueClient queueClient = account.createCloudQueueClient();
				// get a queue reference to an existing queue
				queue = queueClient.getQueueReference(queueName);
				// if we are here - we are connected to the queue
				isConnected = queue.exists();
				logger.info(String.format("connected to azure queue: %s", queueName));
			}
		}
		catch (Exception e)
		{
			logger.fatal(String.format("error connecting to azure queue: %s", e.getMessage()));
		}
		return isConnected;
	}

	/**
	 * returns the next numOfMessagesToRead from the queue, or null if not connected to the queue. this function is
	 * BLOCKING
	 */
	public List<String> getMessages()
	{
		{
			// if not connected, try to reconnect. if succeeded, or was connected - get messages from the queue
			if ((!isConnected) && (!connect()))
			{
				return null;
			}

			logger.info(String.format("waiting for messages from azure queue %s...", queueName));
			// try to read messages from the queue, block until there are messages to read
			Iterable<CloudQueueMessage> messages = null;
			do
			{
				try
				{
					Thread.sleep(sleepBetweenDequeuesMsec);
					messages = queue.retrieveMessages(numOfMessagesToRead, messageInvisiblePeriodSec, null, null);
				}
				catch (Exception ex)
				{
					logger.fatal(String.format("error getting messages from azure queue (%s): error data: %s",
							queueName, ex.getMessage()));
				}
			}
			while ((messages == null) || (!messages.iterator().hasNext()));

			// go over the messages, extract the events from each message and delete the message.
			// a message will be deleted from the queue only if its content was added to the messages to return
			// list, otherwise it will be read again later
			List<String> events = new ArrayList<String>();
			for (CloudQueueMessage cloudQueueMessage : messages)
			{
				try
				{
					String messageContent = cloudQueueMessage.getMessageContentAsString();
					if ((messageContent != null) && (messageContent.length() > 0))
					{
						events.add(messageContent);
					}
					// delete the message (empty messages will also be deleted)
					queue.deleteMessage(cloudQueueMessage);
				}
				catch (Exception ex)
				{
					logger.fatal(String.format("error while reading messages from azure queue (%s): error data: %s",
							queueName, ex.getMessage()));
				}
			}
			
			logger.info(String.format("got %d messages from azure queue (%s)", events.size(), queueName));
			return events;
		}
	}
}
