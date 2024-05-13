package com.github.spring.batch.writer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.github.spring.batch.util.CounterUtils;
import org.apache.commons.collections.KeyValue;
import org.apache.commons.collections.keyvalue.DefaultMapEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

/**
 * Writes data.
 */
public abstract class DataMessageWriter implements ItemWriter<List<KeyValue>> {
	private Logger log = LoggerFactory.getLogger(DataMessageWriter.class);

	@SuppressWarnings("unchecked")
	public void write(List<? extends List<KeyValue>> listOfListOfKeyValues) throws IOException, ExecutionException, InterruptedException {
		Chunk<? extends List<KeyValue>> chunk = new Chunk();
		chunk.addAll((List)listOfListOfKeyValues);
		write(chunk);
	}

	@Override
	public void write(Chunk<? extends List<KeyValue>> chunk)
			throws IOException, ExecutionException, InterruptedException {

		for (List<KeyValue> listOfKeyValues : chunk) {
			for (KeyValue kv : listOfKeyValues) {
				DefaultMapEntry messageKey = (DefaultMapEntry) kv.getKey();
				log.info("write message for : key=[{}] ", messageKey.getKey().toString());
				send((byte[]) kv.getValue());
			}			
		}
	}

	/**
	 * Sends message. Override the method with implemented message sending.
	 * @param message
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	protected void send(byte[] message) throws InterruptedException, ExecutionException, IOException {
		CounterUtils.updateTotalMessageCount();
		System.out.println("Message sent: " + message); // Sends message to the standard out.
		log.info("completed sending message [{}] ", message);
	}
} 