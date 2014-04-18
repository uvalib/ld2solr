/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Sets.difference;
import static com.hp.hpl.jena.query.ReadWrite.READ;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static com.hp.hpl.jena.tdb.TDBFactory.createDataset;
import static edu.virginia.lib.ld2solr.spi.Stage.DEFAULT_NUM_THREADS;
import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.io.Files;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.impl.FilesystemPersister;
import edu.virginia.lib.ld2solr.impl.IndexRun;
import edu.virginia.lib.ld2solr.impl.JenaBackend;
import edu.virginia.lib.ld2solr.impl.SolrXMLOutputStage;
import edu.virginia.lib.ld2solr.spi.OutputStage;
import edu.virginia.lib.ld2solr.spi.RecordSink.RecordPersister;

/**
 * @author ajs6f
 * 
 */
public class Main {

	private Dataset dataset;

	private CacheAssembler cacheAssembler = null;

	private RecordPersister<?> persister;

	private OutputStage<?> outputStage = null;

	private Integer numIndexerThreads = DEFAULT_NUM_THREADS;

	private static final Logger log = getLogger(Main.class);

	public void fullRun(final String transformation, final Set<Resource> uris, final Set<Resource> successfullyRetrieved) {

		// first, we may need to assemble the cache of RDF
		if (cacheAssembler != null) {
			successfullyRetrieved.addAll(cacheAssembler.call());
			final Set<Resource> failures = difference(uris, successfullyRetrieved);
			if (failures.size() > 0) {
				log.warn("Failed to retrieve some resources!");
				for (final Resource failure : failures) {
					log.warn("Resource: {}", failure);
				}
			}
		}
		dataset.begin(READ);
		log.trace("Operating with triples:\n{}", dataset.getDefaultModel());
		dataset.end();

		log.info("Writing to output location: {}", persister.location());

		final IndexRun indexRun = new IndexRun(transformation, successfullyRetrieved, JenaBackend.with(dataset),
				numIndexerThreads);

		// workflow!
		indexRun.andThen(outputStage);
		outputStage.andThen(persister);
		indexRun.run();

	}

	/**
	 * @param dataset
	 *            the {@link Model} to use underneath the RDF cache
	 * @return this {@link Main} for continued operation
	 */
	public Main dataset(final Dataset d) {
		this.dataset = d;
		return this;
	}

	/**
	 * @param ca
	 *            the {@link CacheAssembler} to use
	 * @return this {@link Main} for continued operation
	 */
	public Main assembler(final CacheAssembler ca) {
		this.cacheAssembler = ca;
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
	 */
	public static void main(final String[] args) throws ParseException, IOException {
		final CommandLine cmd = new BasicParser().parse(getOptions(), args);
		if (cmd.hasOption('h')) {
			new HelpFormatter().printHelp("ld2solr -t transform-file -o output-dir -u input-uris", getOptions());
		} else {
			if (!cmd.hasOption('u') || !cmd.hasOption('t') || !cmd.hasOption('o')) {
				new HelpFormatter().printHelp("ld2solr -t transform-file -o output-dir -u input-uris", getOptions());
				throw new IllegalArgumentException(
						"This utility requires a list of URIs to index, a transform to use for indexing and a place to put the index records!\n");
			}
			final String separator = cmd.hasOption('s') ? cmd.getOptionValue('s') : "\n";
			final File uriFile = new File(cmd.getOptionValue('u'));
			log.info("Using URI file: {}", uriFile.getAbsolutePath());
			final Set<Resource> uris = new HashSet<>(transform(asList(Files.toString(uriFile, UTF_8).split(separator)),
					string2uri));
			final File transformFile = new File(cmd.getOptionValue("transform"));
			log.info("Using transform file: {}", transformFile.getAbsolutePath());
			final String transform = Files.toString(transformFile, UTF_8);
			final String outputDirectory = cmd.getOptionValue('o');
			log.info("Using output directory: {}", outputDirectory);
			final Main main = new Main();
			if (cmd.hasOption('c')) {
				final String cacheFile = cmd.getOptionValue('c');
				log.info("Using cache location: {}", cacheFile);
				final Dataset dataset = createDataset(cacheFile);
				dataset.begin(READ);
				main.dataset = dataset;
				dataset.end();
			} else {
				main.dataset = createDataset();
			}
			if (!cmd.hasOption("skip-retrieval")) {
				Integer assemblerThreads = DEFAULT_NUM_THREADS;
				if (cmd.hasOption("assembler-threads")) {
					assemblerThreads = parseInt(cmd.getOptionValue("assembler-threads"));
				}
				main.assembler(new CacheAssembler(main.dataset, assemblerThreads).uris(uris));
			} else if (!cmd.hasOption('c')) {
				final String bangLine = repeat("!", 80);
				log.warn(bangLine);
				log.warn("Operating over empty in-memory cache without retrieval step to fill it!");
				log.warn(bangLine);
			}
			if (cmd.hasOption("indexing-threads")) {
				main.numIndexerThreads = parseInt(cmd.getOptionValue("indexing-threads"));
			}
			final Set<Resource> successfullyRetrieved = new HashSet<>(uris.size());
			main.outputStage(new SolrXMLOutputStage());
			main.persister(new FilesystemPersister().location(outputDirectory));
			main.fullRun(transform, uris, successfullyRetrieved);
			log.debug("Successfully retrieved URIs: {}", successfullyRetrieved);
		}
	}

	private static Options getOptions() {
		return new Options()
				.addOption("u", "uris", true, "(Required) A file or pipe with a list of URIs to index.")
				.addOption("o", "output-dir", true, "(Required) Location into which to place output files.")
				.addOption("t", "transform", true,
						"(Required) Location of LDPath transform with which to create index records. ")
				.addOption("c", "cache", true,
						"Location of persistent triple cache. (Defaults to in-memory only operation.)")
				.addOption(
						"sr",
						"skip-retrieval",
						false,
						"Should retrieval and caching of Linked Data resources before indexing stages be skipped?"
								+ "If set, option for persistent triple cache must be supplied or "
								+ "indexing stages will operate over an empty cache.")
				.addOption("s", "separator", true, "Separator between input URIs. (Defaults to \\n.)")
				.addOption(
						new Option(null, "assembler-threads", true,
								"The number of threads to use for RDF cache accumulation. (Defaults to "
										+ DEFAULT_NUM_THREADS + ".)"))
				.addOption(
						new Option(null, "indexing-threads", true,
								"The number of threads to use for indexing operation. (Defaults to "
										+ DEFAULT_NUM_THREADS + ".)"))
				.addOption("h", "help", false, "Print this help message.");
	}

	private static Function<String, Resource> string2uri = new Function<String, Resource>() {

		@Override
		public Resource apply(final String uri) {
			return createResource(uri);
		}
	};
}
