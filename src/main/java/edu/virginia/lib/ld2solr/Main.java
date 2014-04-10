/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.collect.Sets.difference;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.impl.JenaBackend;

/**
 * @author ajs6f
 * 
 */
public class Main {

	private final Model model = createDefaultModel();

	private final JenaBackend cache = new JenaBackend(model);

	private static final Logger log = getLogger(Main.class);

	public void fullRun(final String transformation, final Set<Resource> uris, final Set<Resource> successfullyRetrieved)
			throws InterruptedException, ExecutionException {
		successfullyRetrieved.addAll(new CacheAssembler(model, uris).call());
		final Set<Resource> failures = difference(uris, successfullyRetrieved);
		if (failures.size() > 0) {
			log.warn("Failed to retrieve some resources!");
			for (final Resource failure : failures) {
				log.warn("Resource: {}", failure);
			}
		}
		log.debug("Operating with RDF cache: {}", model.getGraph());
		final Iterator<Future<NamedFields>> records = new IndexRun(transformation, successfullyRetrieved, cache).get();
		while (records.hasNext()) {
			log.info("Retrieved index record:\n{}", records.next().get());
		}
	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {

	}
}
