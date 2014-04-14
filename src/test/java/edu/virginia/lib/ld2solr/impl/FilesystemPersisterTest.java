package edu.virginia.lib.ld2solr.impl;

import static com.google.common.io.Files.createTempDir;
import static java.io.File.separator;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.junit.Before;
import org.junit.Test;

import edu.virginia.lib.ld2solr.api.OutputRecord;

public class FilesystemPersisterTest {

	protected static final String TEST_ID = "FakeRecord";

	private FilesystemPersister testPersister;

	private final OutputRecord outputRecord = new OutputRecord() {

		@Override
		public String id() {
			return TEST_ID;
		}

		@Override
		public byte[] record() {
			return new byte[0];
		}
	};

	@Before
	public void setUp() {
		testPersister = new FilesystemPersister();
	}

	@Test
	public void testAccept() throws UnsupportedEncodingException {
		final String location = createTempDir().getAbsolutePath();
		testPersister.location(location);
		assertEquals("Stored wrong location for persistence!", location, testPersister.location());
		testPersister.accept(outputRecord);
		final File expectedFile = new File(location + separator + URLDecoder.decode(TEST_ID, "UTF-8"));
		assertTrue("Didn't find persisted record!", asList(new File(location).listFiles()).contains(expectedFile));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testAndThen() {
		testPersister.andThen(null);
		fail("Should not have been able to set a following stage for FilesystemPersister!");
	}

}
