package edu.virginia.lib.ld2solr.impl;

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.marmotta.ldpath.backend.jena.GenericJenaBackend;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Resource;

public class LDPathIndexerHeadTest {

	private LDPathIndexerHead testIndexerHead;

	private final Reader mockReader = new StringReader("");
	private final Reader mockBadReader = new StringReader("THIS IS NOT A LEGITIMATE LDPATH PROGRAM!");

	private GenericJenaBackend mockLDPersistentBackend;

	private Resource mockURI;

	@Before
	public void setUp() throws IOException {
		initMocks(this);
		mockLDPersistentBackend = new GenericJenaBackend(createDefaultModel());
		testIndexerHead = new LDPathIndexerHead(mockLDPersistentBackend, mockReader);
	}

	@Test
	public void testApply() {
		assertEquals("Should not have retrieved fields from empty transform and cache!", 0,
				testIndexerHead.apply(mockURI).size());
	}

	@Test(expected = RuntimeException.class)
	public void testBadTransform() {
		testIndexerHead = new LDPathIndexerHead(mockLDPersistentBackend, mockBadReader);
		testIndexerHead.apply(mockURI);
		fail("Should not have been able to operate with ill-formed transform!");
	}

}
