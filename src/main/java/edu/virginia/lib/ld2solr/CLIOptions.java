package edu.virginia.lib.ld2solr;

import static edu.virginia.lib.ld2solr.spi.ThreadedStage.DEFAULT_NUM_THREADS;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Options for use with ld2solr CLI.
 * 
 * @author ajs6f
 * 
 */
public enum CLIOptions {
	URIS {
		@Override
		public Option getOption() {
			return new Option("u", "uris", true, "(Required) A file or pipe with a list of URIs to index.");
		}
	},
	OUTPUTDIR {
		@Override
		public Option getOption() {
			return new Option("o", "output-dir", true, "(Required) Location into which to place output files.");
		}
	},
	TRANSFORM {
		@Override
		public Option getOption() {
			return new Option("t", "transform", true,
					"(Required) Location of LDPath transform with which to create index records. ");
		}
	},
	CACHE {
		@Override
		public Option getOption() {
			return new Option("c", "cache", true,
					"Location of persistent triple cache. (Defaults to in-memory only operation.)");
		}
	},
	ACCEPT {
		@Override
		public Option getOption() {
			return new Option("a", "accept", true, "HTTP 'Accept:' header to use. (Defaults to none.)");
		}
	},
	SKIPRETRIEVAL {
		@Override
		public Option getOption() {
			return new Option("sr", "skip-retrieval", false,
					"Should retrieval and caching of Linked Data resources before indexing stages be skipped?"
							+ "If set, option for persistent triple cache must be supplied or "
							+ "indexing stages will operate over an empty cache.");
		}
	},
	SEPARATOR {
		@Override
		public Option getOption() {
			return new Option("s", "separator", true, "Separator between input URIs. (Defaults to \\n.)");
		}
	},
	ASSEMBLERTHREADS {
		@Override
		public Option getOption() {
			return new Option(null, "assembler-threads", true,
					"The number of threads to use for RDF cache accumulation. (Defaults to " + DEFAULT_NUM_THREADS
							+ ".)");
		}
	},
	INDEXINGTHREADS {
		@Override
		public Option getOption() {
			return new Option(null, "indexing-threads", true,
					"The number of threads to use for indexing operation. (Defaults to " + DEFAULT_NUM_THREADS + ".)");
		}
	},
	HELP {
		@Override
		public Option getOption() {
			return new Option("h", "help", false, "Print this help message.");
		}
	};

	public abstract Option getOption();

	public static String helpLine = "ld2solr -t transform-file -o output-dir -u input-uris";

	public static Options getOptions() {
		final Options options = new Options();
		for (final CLIOptions option : CLIOptions.values()) {
			options.addOption(option.getOption());
		}
		return options;
	}
}
