package edu.virginia.lib.ld2solr;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Sets.difference;
import static com.hp.hpl.jena.query.ReadWrite.READ;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static com.hp.hpl.jena.tdb.TDBFactory.createDataset;
import static edu.virginia.lib.ld2solr.CLIOption.ACCEPT;
import static edu.virginia.lib.ld2solr.CLIOption.ASSEMBLERTHREADS;
import static edu.virginia.lib.ld2solr.CLIOption.CACHE;
import static edu.virginia.lib.ld2solr.CLIOption.INDEXINGTHREADS;
import static edu.virginia.lib.ld2solr.CLIOption.OUTPUTDIR;
import static edu.virginia.lib.ld2solr.CLIOption.SKIPRETRIEVAL;
import static edu.virginia.lib.ld2solr.CLIOption.TRANSFORM;
import static edu.virginia.lib.ld2solr.CLIOption.URIS;
import static edu.virginia.lib.ld2solr.CLIOption.helpLine;
import static edu.virginia.lib.ld2solr.CLIOption.helpOptions;
import static edu.virginia.lib.ld2solr.CLIOption.mainOptions;
import static edu.virginia.lib.ld2solr.spi.ThreadedStage.DEFAULT_NUM_THREADS;
import static java.lang.Integer.parseInt;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.marmotta.ldpath.LDPath;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.io.Files;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.impl.FilesystemPersister;
import edu.virginia.lib.ld2solr.impl.IndexRun;
import edu.virginia.lib.ld2solr.impl.JenaBackend;
import edu.virginia.lib.ld2solr.impl.SolrXMLOutputStage;
import edu.virginia.lib.ld2solr.spi.CacheLoader;
import edu.virginia.lib.ld2solr.spi.OutputStage;
import edu.virginia.lib.ld2solr.spi.RecordSink.RecordPersister;

/**
 * Assembles and operates an indexing workflow from SPI implementations, as well
 * as supplying CLI support therefor.
 * 
 * @author ajs6f
 * 
 */
public class Workflow {

	/*
	 * because LDPath uses java.util.ServiceLoader to search the context
	 * classpath, we need to explicitly call out LDPath here to make sure it
	 * gets initialized properly
	 */
	@SuppressWarnings("unused")
	private final LDPath<String> testTransform = new LDPath<>(null);

	private Dataset dataset;

	private CacheLoader<?, Dataset> cacheAssembler = null;

	private RecordPersister persister;

	private OutputStage outputStage = null;

	private Integer numIndexerThreads = DEFAULT_NUM_THREADS;

	private static final Logger log = getLogger(Workflow.class);

	/**
	 * Loads the cache for indexing workflow.
	 * 
	 * @param uris
	 *            URIs of resources to load into cache.
	 * @return those URIs that were successfully loaded
	 * @throws InterruptedException
	 */
	public Set<Resource> cache(final Set<Resource> uris) throws InterruptedException {
		final Set<Resource> successfullyRetrieved = cacheAssembler.load(uris);
		final Set<Resource> failures = difference(uris, successfullyRetrieved);
		if (failures.size() > 0) {
			log.error("Failed to retrieve some resources!");
			for (final Resource failure : failures) {
				log.warn("Resource: {} could not be retrieved!", failure);
			}
		}
		cacheAssembler.shutdown();
		return successfullyRetrieved;
	}

	/**
	 * Performs an index run over the cache.
	 * 
	 * @param transformation
	 *            The LDPath transformation to use for indexing
	 * @param uris
	 *            URIs to index
	 * @throws InterruptedException
	 * @see <a href="http://marmotta.apache.org/ldpath/">LDPath</a>
	 */
	public void index(final String transformation, final Set<Resource> uris) throws InterruptedException {

		log.info("Using transformation:\n{}", transformation);

		dataset.begin(READ);
		log.trace("Operating with triples:\n{}", dataset.getDefaultModel());
		dataset.end();

		log.info("Writing to output location: {}", persister.location());

		final IndexRun indexRun = new IndexRun(transformation, uris, JenaBackend.with(dataset))
				.threads(numIndexerThreads);

		// workflow!
		indexRun.andThen(outputStage).andThen(persister);
		indexRun.run();
		indexRun.shutdown();
		outputStage.shutdown();
		persister.shutdown();
	}

	/**
	 * Assigns a {@link Dataset} to use as cache.
	 * 
	 * @param d
	 *            the {@link Dataset} to use underneath the RDF cache
	 * @return this {@link Workflow} for continued operation
	 */
	public Workflow dataset(final Dataset d) {
		this.dataset = d;
		return this;
	}

	/**
	 * Assigns a {@link CacheLoader} to load the cache.
	 * 
	 * @param ca
	 *            the {@link DatasetCacheAssembler} to use
	 * @return this {@link Workflow} for continued operation
	 */
	public Workflow assembler(final CacheLoader<?, Dataset> ca) {
		this.cacheAssembler = ca;
		return this;
	}

	/**
	 * Assigns a {@link RecordPersister} to use for the last stage of workflow.
	 * 
	 * @param rp
	 *            the {@link RecordPersister} to use
	 * @return this {@link Workflow} for continued operation
	 */
	public Workflow persister(final RecordPersister rp) {
		this.persister = rp;
		return this;
	}

	/**
	 * Assigns an {@link OutputStage} with which to produce output records.
	 * 
	 * @param os
	 *            the {@link OutputStage} to use
	 * @return this {@link Workflow} for continued operation
	 */
	public Workflow outputStage(final OutputStage os) {
		this.outputStage = os;
		return this;
	}

	/**
	 * CLI entry method.
	 * 
	 * @param args
	 *            command-line arguments
	 * @throws ParseException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws ParseException, IOException, InterruptedException {
		final Parser cliParser = new BasicParser();
		// first check to see if this is an invocation of help
		final CommandLine helpCmd = cliParser.parse(helpOptions, args, true);
		if (helpCmd.getOptions().length > 0) {
			new HelpFormatter().printHelp(helpLine, mainOptions);
		} else {
			// it's not a help invocation, so actually parse the args
			try {
				final CommandLine cmd = cliParser.parse(mainOptions, args);

				final File uriFile = new File(cmd.getOptionValue(URIS.opt()));
				log.info("Using URI file: {}", uriFile.getAbsolutePath());
				final Set<Resource> uris = retrieveUrisFromFile(uriFile);

				final File transformFile = new File(cmd.getOptionValue(TRANSFORM.opt()));
				final String transform = Files.toString(transformFile, UTF_8);

				final String outputDirectory = cmd.getOptionValue(OUTPUTDIR.opt());

				final Workflow main = new Workflow();
				if (cmd.hasOption(CACHE.opt())) {
					final String cacheFile = cmd.getOptionValue(CACHE.opt());
					log.info("Using cache location: {}", cacheFile);
					final Dataset dataset = createDataset(cacheFile);
					dataset.begin(READ);
					main.dataset = dataset;
					dataset.end();
				} else {
					main.dataset = createDataset();
				}
				Set<Resource> urisToIndex;
				if (!cmd.hasOption(SKIPRETRIEVAL.opt())) {
					Integer assemblerThreads = DEFAULT_NUM_THREADS;
					if (cmd.hasOption(ASSEMBLERTHREADS.opt())) {
						assemblerThreads = parseInt(cmd.getOptionValue(ASSEMBLERTHREADS.opt()));
					}
					final DatasetCacheAssembler assembler = new DatasetCacheAssembler().cache(main.dataset).threads(
							assemblerThreads);
					if (cmd.hasOption(ACCEPT.opt())) {
						final String accept = cmd.getOptionValue(ACCEPT.opt());
						log.info("Requesting HTTP Content-type: {} for resource retrieval.", accept);
						assembler.accepts(accept);
					}
					main.assembler(assembler);
					urisToIndex = main.cache(uris);
					log.debug("Successfully retrieved URIs:\n{}", urisToIndex);
				} else {
					urisToIndex = uris;
					if (!cmd.hasOption(CACHE.opt())) {
						final String bangLine = repeat("!", 80);
						log.warn(bangLine);
						log.warn("Operating over empty in-memory cache without retrieval step to fill it!");
						log.warn(bangLine);
					}
				}
				if (cmd.hasOption(INDEXINGTHREADS.opt())) {
					main.numIndexerThreads = parseInt(cmd.getOptionValue(INDEXINGTHREADS.opt()));
				}
				main.outputStage(new SolrXMLOutputStage());
				main.persister(new FilesystemPersister().location(outputDirectory));
				main.index(transform, urisToIndex);
			} catch (final ParseException e) {
				new HelpFormatter().printHelp(helpLine, mainOptions);
				throw e;
			}
		}
	}

	private static Set<Resource> retrieveUrisFromFile(final File f) throws IOException {
		return new HashSet<>(transform(Files.readLines(f, UTF_8), new Function<String, Resource>() {

			@Override
			public Resource apply(final String uri) {
				return createResource(uri);
			}
		}));
	}
}
