package edu.virginia.lib.ld2solr.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.hp.hpl.jena.rdf.model.Model;

public class JenaBackendTest {

	@Mock
	private Model mockModel;

	private JenaBackend testBackend;

	@Before
	public void setUp() {
		initMocks(this);
		testBackend = new JenaBackend(mockModel);
	}

	@Test
	public void testCanRetrieveUnderlyingModel() {
		assertEquals("Unable to retrieve underlying model!", mockModel, testBackend.model());
	}

}
