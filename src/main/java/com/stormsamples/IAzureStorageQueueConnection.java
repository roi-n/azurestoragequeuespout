package com.stormsamples;

import java.util.List;

/**
 * an interface to Azure Storage Queues
 * @author roin
 *
 */
public interface IAzureStorageQueueConnection {

	public String getConnectionString();

	public String getQueueName();

	public Long getApproximateMessageCount();

	public boolean isConnected();

	public boolean connect();

	public List<String> getMessages();
}
