/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import edu.virginia.lib.ld2solr.api.NamedFields;

/**
 * @author ajs6f
 * 
 */
public class IndexRunTest {

	private IndexRun testIndexRun;

	private static final String uri = "https://www.flickr.com/photos/mhausenblas/1059656723/";

	private static final Set<Resource> uris = ImmutableSet.of(ResourceFactory.createResource(uri));

	private static final String mockTransform = "title = dc:title :: xsd:string;";

	private static final Logger log = getLogger(IndexRunTest.class);

	@Before
	public void setUp() {
		testIndexRun = new IndexRun(mockTransform, uris);
	}

	@Test
	public void testRun() throws InterruptedException, ExecutionException {
		final Iterator<Future<NamedFields>> results = testIndexRun.get();
		assertTrue("Failed to retrieve any results!", results.hasNext());
		final NamedFields firstResult = testIndexRun.get().next().get();
		log.info("Created index record: {}", firstResult);
		assertTrue("Failed to create title in test index record!", firstResult.containsKey("title"));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBadOperation() {
		testIndexRun.get().remove();
	}
}
