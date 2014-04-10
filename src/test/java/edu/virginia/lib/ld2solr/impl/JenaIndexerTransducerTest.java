package edu.virginia.lib.ld2solr.impl;

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Resource;

public class JenaIndexerTransducerTest {

	private JenaIndexingTransducer testIndexerHead;

	private final String mockTransform = "";

	private final String mockBadTransform = "THIS IS NOT A LEGITIMATE LDPATH PROGRAM!";

	private JenaBackend mockLDPersistentBackend;

	private Resource mockURI;

	@Before
	public void setUp() {
		initMocks(this);
		mockLDPersistentBackend = new JenaBackend(createDefaultModel());
		testIndexerHead = new JenaIndexingTransducer(mockLDPersistentBackend, mockTransform);
	}

	@Test
	public void testApply() {
		assertEquals("Should not have retrieved fields from empty transform and cache!", 0,
				testIndexerHead.apply(mockURI).size());
	}

	@Test(expected = RuntimeException.class)
	public void testBadTransform() {
		testIndexerHead = new JenaIndexingTransducer(mockLDPersistentBackend, mockBadTransform);
		testIndexerHead.apply(mockURI);
		fail("Should not have been able to operate with ill-formed transform!");
	}

	@Test
	public void testCanRetrieveUnderlyingTransformAndCache() {
		testIndexerHead = new JenaIndexingTransducer(mockLDPersistentBackend, mockTransform);
		assertEquals("Could not retrieve underlying cache!", mockLDPersistentBackend, testIndexerHead.cache());
		assertEquals("Could not retrieve underlying transform!", mockTransform, testIndexerHead.transformation());
	}

}
