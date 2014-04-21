package edu.virginia.lib.ld2solr.impl;

import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.Before;
import org.mockito.Mock;

public class LDPathIndexerTest {

	@Mock
	private JenaBackend mockLDPersistentBackend;

	@Before
	public void setUp() {
		initMocks(this);
		new LDPathIndexer(mockLDPersistentBackend);
	}
}
