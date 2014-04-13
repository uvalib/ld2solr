/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Set;

import org.apache.marmotta.ldpath.LDPath;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import edu.virginia.lib.ld2solr.api.NamedFields;

/**
 * @author ajs6f
 * 
 */
public class IndexRunTest extends TestHelper {

	private IndexRun testIndexRun;

	private TestAcceptor<NamedFields, ?> acceptor;

	private static final String mockTransform = "title = dc:title :: xsd:string;";

	private static final Logger log = getLogger(IndexRunTest.class);

	private static final long TIMEOUT = 10000;

	private static final long TIMESTEP = 1000;

	@Before
	public void setUp() {
		/*
		 * because JUnit uses reflection to set up the classpath for a test and
		 * LDPath uses java.util.ServiceLoader to search the context classpath,
		 * we need to explicitly call out LDPath here to make sure it gets
		 * initialized properly
		 */
		new LDPath<String>(null);
		testIndexRun = new IndexRun(mockTransform, uris);
		acceptor = new TestAcceptor<NamedFields, Void>();
		testIndexRun.andThen(acceptor);
	}

	@Test
	public void testRun() throws InterruptedException {
		testIndexRun.run();
		final long startTime = currentTimeMillis();
		synchronized (acceptor) {
			while (acceptor.accepted().size() < uris.size() && currentTimeMillis() < (startTime + TIMEOUT)) {
				acceptor.wait(TIMESTEP);
			}
		}
		final Set<NamedFields> results = acceptor.accepted();
		assertEquals("Didn't receive as many index records as input resources!", uris.size(), results.size());
		for (final NamedFields result : results) {
			log.info("Created index record: {}", result);
			assertTrue("Failed to create title in each test index record!", result.containsKey("title"));
		}
	}
}
