/**
 * 
 */
package edu.virginia.lib.ld2solr.impl;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;

import com.google.common.io.Files;

import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.RecordSink.RecordPersister;

/**
 * @author ajs6f
 * 
 */
public class FilesystemPersister implements RecordPersister<FilesystemPersister> {

	File directory;

	private static final Logger log = getLogger(FilesystemPersister.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.virginia.lib.ld2solr.spi.RecordSink#accept(edu.virginia.lib.ld2solr
	 * .api.OutputRecord)
	 */
	@Override
	public void accept(final OutputRecord record) {
		final File file = new File(directory, record.id());
		log.debug("Persisting to: {}", file.toString());
		try {
			Files.asByteSink(file).write(record.record());
		} catch (final IOException e) {
			log.error("Error persisting: {}!", record.id());
			log.error("Exception: ", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.virginia.lib.ld2solr.spi.RecordSink.RecordPersister#location(java
	 * .lang.String)
	 */
	@Override
	public FilesystemPersister location(final String location) {
		directory = new File(location);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.virginia.lib.ld2solr.spi.RecordSink.RecordPersister#location()
	 */
	@Override
	public String location() {
		return directory.getAbsolutePath();
	}

	@Override
	public void andThen(final Acceptor<OutputRecord, ?> a) {
		throw new UnsupportedOperationException();
	}

}
