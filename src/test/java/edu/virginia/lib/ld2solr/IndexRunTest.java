/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

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

	private static final Reader mockTransform = new StringReader("title = dc:title :: xsd:string;");

	@Before
	public void setUp() throws IOException {
		testIndexRun = new IndexRun(mockTransform, uris);
	}

	@Test
	public void testRun() {
		final Iterator<NamedFields> results = testIndexRun.get();
		assertTrue("Failed to retrieve any results!", results.hasNext());
		final NamedFields firstResult = testIndexRun.get().next();
		assertTrue(firstResult.containsKey("title"));
	}
}
