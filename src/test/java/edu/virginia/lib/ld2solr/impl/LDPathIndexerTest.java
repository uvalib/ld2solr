package edu.virginia.lib.ld2solr.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class LDPathIndexerTest {

	private LDPathIndexer testIndexer;

	@Mock
	private JenaBackend mockLDPersistentBackend;

	private String mockTransform;

	@Before
	public void setUp() {
		initMocks(this);
		testIndexer = new LDPathIndexer(mockLDPersistentBackend);
	}

	@Test
	public void testApply() {
		assertEquals("Produced IndexingTransducer with incorrect transformation configured!", mockTransform,
				testIndexer.apply(mockTransform).transformation());
		assertEquals("Produced IndexingTransducer with incorrect cache configured!", mockLDPersistentBackend,
				testIndexer.apply(mockTransform).cache());
	}

}
