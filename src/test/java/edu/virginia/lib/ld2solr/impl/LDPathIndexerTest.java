package edu.virginia.lib.ld2solr.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.Reader;

import org.apache.marmotta.ldpath.backend.jena.GenericJenaBackend;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class LDPathIndexerTest {

	private LDPathIndexer testIndexer;

	@Mock
	private GenericJenaBackend mockLDPersistentBackend;

	@Mock
	private Reader mockTransform;

	@Before
	public void setUp() {
		initMocks(this);
		testIndexer = new LDPathIndexer(mockLDPersistentBackend);
	}

	@Test
	public void testApply() {
		assertEquals("Produced IndexerHead with incorrect transformation configured!", mockTransform, testIndexer
				.apply(mockTransform).transformation());
		assertEquals("Produced IndexerHead with incorrect cache configured!", mockLDPersistentBackend, testIndexer
				.apply(mockTransform).cache());
	}

}
