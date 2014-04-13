/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Sets.difference;
import static com.google.common.io.Files.createTempDir;
import static com.hp.hpl.jena.query.ReadWrite.READ;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static com.hp.hpl.jena.tdb.TDBFactory.createDataset;
import static java.lang.System.err;
import static java.util.Arrays.asList;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.io.Files;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.impl.FilesystemPersister;
import edu.virginia.lib.ld2solr.impl.JenaBackend;
import edu.virginia.lib.ld2solr.spi.OutputStage;
import edu.virginia.lib.ld2solr.spi.RecordSink.RecordPersister;

/**
 * @author ajs6f
 * 
 */
public class Main {

	private Model model;

	private RecordPersister<?> persister;

	private OutputStage<?> outputStage = null;

	private static final Logger log = getLogger(Main.class);

	public void fullRun(final String transformation, final Set<Resource> uris, final Set<Resource> successfullyRetrieved)
			throws InterruptedException {

		// first, we assemble the cache of RDF
		successfullyRetrieved.addAll(new CacheAssembler(model, uris).call());
		final Set<Resource> failures = difference(uris, successfullyRetrieved);
		if (failures.size() > 0) {
			log.warn("Failed to retrieve some resources!");
			for (final Resource failure : failures) {
				log.warn("Resource: {}", failure);
			}
		}
		log.debug("Operating with triples:\n{}", model.getGraph());

		log.info("Writing to: {}", persister.location());

		final IndexRun indexRun = new IndexRun(transformation, successfullyRetrieved, new JenaBackend(model));

		// workflow!
		indexRun.andThen(outputStage);
		outputStage.andThen(persister);
		indexRun.run();

	}

	/**
	 * @param model
	 *            the {@link Model} to use underneath the RDF cache
	 * @return this {@link Main} for continued operation
	 */
	public Main model(final Model model) {
		this.model = model;
		return this;
	}

	/**
	 * @param persister
	 *            the {@link RecordPersister} to use
	 * @return this {@link Main} for continued operation
	 */
	public Main persister(final RecordPersister<?> persister) {
		this.persister = persister;
		return this;
	}

	/**
	 * @param persister
	 *            the {@link RecordPersister} to use
	 * @return this {@link Main} for continued operation
	 */
	public Main outputStage(final OutputStage<?> stage) {
		this.outputStage = stage;
		return this;
	}

	/**
	 * @param args
	 * @throws ParseException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws ParseException, IOException, InterruptedException {
		final CommandLine cmd = new BasicParser().parse(getOptions(), args);
		if (cmd.hasOption("help")) {
			new HelpFormatter().printHelp("ld2solr: an indexing utility for Linked Data", getOptions());
		}
		if (!cmd.hasOption("uris") || !cmd.hasOption("transform")) {
			err.println("This utility requires a list of URIs to index and a transform to use for indexing!\n"
					+ "(Use -h or --help for help.)");
		} else {
			final Set<Resource> uris = new HashSet<>(transform(
					asList(Files.toString(new File(cmd.getOptionValue("uris")), UTF_8).split("\n")), string2uri));
			final String transform = Files.toString(new File(cmd.getOptionValue("transform")), UTF_8);
			final Main main = new Main();
			if (cmd.hasOption("cache")) {
				final Dataset dataset = createDataset(cmd.getOptionValue("cache"));
				dataset.begin(READ);
				main.model = dataset.getDefaultModel();
				dataset.end();
			}
			final Set<Resource> successfullyRetrieved = new HashSet<>(uris.size());
			main.outputStage(new SolrLDOutputStage());
			main.persister(new FilesystemPersister().location(createTempDir().getAbsolutePath()));
			main.fullRun(transform, uris, successfullyRetrieved);
		}
	}

	private static Options getOptions() {
		return new Options()
				.addOption("u", "uris", true, "(Required) A file or pipe with a list of URIs to index.")
				.addOption("h", "help", false, "Print this help message.")
				.addOption("t", "transform", true,
						"(Required) Location of LDPath transform with which to create index records. ")
				.addOption("c", "cache", false,
						"Location of persistent triple cache. (Defaults to in-memory only operation.)");

	}

	private static Function<String, Resource> string2uri = new Function<String, Resource>() {

		@Override
		public Resource apply(final String uri) {
			return createResource(uri);
		}
	};
}
