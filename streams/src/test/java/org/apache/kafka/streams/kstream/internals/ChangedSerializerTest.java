package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;

public class ChangedSerializerTest {

	private final StringSerializer ss = new StringSerializer();

	private ChangedSerializer<String> changedSerializer;

	@Before
	public void setUp() {
		changedSerializer = new ChangedSerializer<>(ss);
	}

	@Test
	public void testInner() {
		changedSerializer.setInner(ss);

		Serializer<String> expected = changedSerializer.inner();

		assertSame("The inner serializer is not the same", ss, expected);
	}

	@Test(expected = StreamsException.class)
	public void testSerializeWithOldAndNewValueOfChangeNotNullShouldThrowStreamsException() {
		final Change<String> data = new Change<>("new", "old");

		changedSerializer.serialize("test", data);
	}

	@Test(expected = StreamsException.class)
	public void testSerializeWithOldAndNewValueOfChangeNullShouldThrowStreamsException() {
		final Change<String> data = new Change<>(null, null);

		changedSerializer.serialize("test", data);
	}

	@Test
	public void testSerializeWithOldValueOfChangeNull() {
		final String newValue = "new";
		byte[] serializedKey = ss.serialize("test", newValue);

		byte[] expected = ByteBuffer.allocate(serializedKey.length + 1)
			.put(serializedKey)
			.put((byte) 1)
			.array();
		
		final Change<String> data = new Change<>(newValue, null);
		byte[] serialized = changedSerializer.serialize("test", data);
		
		assertArrayEquals(expected, serialized);
	}

	@Test
	public void testSerializeWithNewValueOfChangeNull() {
		final String oldValue = "old";
		byte[] serializedKey = ss.serialize("test", oldValue);

		byte[] expected = ByteBuffer.allocate(serializedKey.length + 1)
				.put(serializedKey)
				.put((byte) 0)
				.array();

		final Change<String> data = new Change<>(null, oldValue);
		byte[] serialized = changedSerializer.serialize("test", data);

		assertArrayEquals(expected, serialized);
	}
}
