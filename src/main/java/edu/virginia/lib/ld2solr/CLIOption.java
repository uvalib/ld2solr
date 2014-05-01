package edu.virginia.lib.ld2solr;

import static edu.virginia.lib.ld2solr.spi.CacheAssembler.DEFAULT_TIMEOUT;
import static edu.virginia.lib.ld2solr.spi.ThreadedStage.DEFAULT_NUM_THREADS;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Options for use with ld2solr CLI.
 * 
 * @author ajs6f
 * 
 */
public enum CLIOption {
	URIS(new Option("u", "uris", true, "(Required) A file or pipe with a list of URIs to index."), true),
	OUTPUTDIR(new Option("o", "output-dir", true, "(Required) Location into which to place output files."), true),
	TRANSFORM(new Option("t", "transform", true,
			"(Required) Location of LDPath transform with which to create index records. "), true),
	CACHE(
			new Option("c", "cache", true,
					"Location of persistent triple cache. (Defaults to in-memory only operation.)")),
	ACCEPT(new Option("a", "accept-header-value", true,
			"HTTP 'Accept:' header to use for HTTP-based Linked Data retrieval. (Defaults to none.)")),
	SKIPRETRIEVAL(new Option("sr", "skip-retrieval", false,
			"Should retrieval and caching of Linked Data resources before indexing stages be skipped?"
					+ "If set, option for persistent triple cache must be supplied or "
					+ "indexing stages will operate over an empty cache.")),
	ASSEMBLERTHREADS(new Option("at", "assembler-threads", true,
			"The number of threads to use for RDF cache accumulation. (Defaults to " + DEFAULT_NUM_THREADS + ".)")),
	INDEXINGTHREADS(new Option("it", "indexing-threads", true,
			"The number of threads to use for indexing operation. (Defaults to " + DEFAULT_NUM_THREADS + ".)")),
	RETRIEVALONTOLOGY(new Option("ont", "retrieval-ontology", true,
			"A file with the OWL ontology to use to establish which predicates should be followed for recursive retrieval. "
					+ "Currently must be in RDF/XML form. (Defaults to no recursive retrieval.")),
	TIMEOUT(new Option("ti", "timeout", true,
			"The length of time to wait for cache assembly to complete, in milliseconds. "
					+ "(Timeout may occur because of unreachable networked resources.) " + "(Defaults to "
					+ DEFAULT_TIMEOUT + " milliseconds.)"));

	public Option option;

	private CLIOption(final Option o, final Boolean... required) {
		this.option = o;
		if (required.length > 0) {
			this.option.setRequired(required[0]);
		} else {
			this.option.setRequired(false);
		}
	}

	/**
	 * @return the short name of this {@link Option} for use with Apace Commons
	 *         CLI facilities.
	 */
	public String opt() {
		return this.option.getOpt();
	}

	/**
	 * Help line to use for CLI help invocation.
	 */
	public static String helpLine = "ld2solr -t transform-file -o output-dir -u input-uris";

	/**
	 * {@link Option}s to trigger CLI help invocation.
	 */
	public static Options helpOptions = new Options().addOption("h", "help", false, "Print this help message.");

	/**
	 * {@link Option}s to use for CLI configuration.
	 */
	public static Options mainOptions;

	static {
		mainOptions = new Options();
		for (final CLIOption option : values()) {
			mainOptions.addOption(option.option);
		}
	}

}
