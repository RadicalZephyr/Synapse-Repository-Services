package org.sagebionetworks.asynchronous.workers.sqs;

import java.util.Collection;
import java.util.List;

import com.amazonaws.services.sqs.model.Message;

/**
 * Abstraction for interacting with Amazon's SQS.
 * 
 * @author jmhill
 *
 */
public interface QueueServiceDao {

	/**
	 * Receive messages from a queue.
	 * @param queueUrl The URL of the queue
	 * @param visibilityTimeoutSec The visibility timeout of each messages (sec) pull from the queue.
	 * @param maxMessages The maximum number of messages that should be pulled from the queue.
	 * @return
	 */
	List<Message> receiveMessages(String queueUrl,
			int visibilityTimeoutSec, int maxMessages);

	/**
	 * Delete the batch of messages.
	 * @param messagesToDelete
	 */
	void deleteMessages(String queueUrl, List<Message> messagesToDelete);
	
	/**
	 * Reset the Visibility timeout of passed messages.
	 * @param newVisibiltySeconds
	 * @param toReset
	 */
	void resetMessageVisibility(String queueUrl, int newVisibiltySeconds, Collection<Message> toReset);



}
