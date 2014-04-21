package edu.virginia.lib.ld2solr.impl;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.slf4j.Logger;

import com.google.common.io.Files;

import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.AbstractStage;
import edu.virginia.lib.ld2solr.spi.RecordSink.RecordPersister;

/**
 * A {@link RecordPersister} that writes {@link OutputRecords} to a filesystem.
 * 
 * @author ajs6f
 * 
 */
public class FilesystemPersister extends AbstractStage<OutputRecord> implements RecordPersister {

	private File directory;

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
		final String fileName;
		try {
			fileName = URLEncoder.encode(record.id(), "UTF-8");
		} catch (final UnsupportedEncodingException e) {
			throw new AssertionError();
		}
		final File file = new File(directory, fileName);
		log.debug("Persisting to: {}", file.toString());
		try {
			Files.asByteSink(file).write(record.record());
			log.debug("Persisted: {} to: {}", record.id(), fileName);
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
		throw new UnsupportedOperationException(
				"It is currently not expected that indexing workflow will continue after persistence.");
	}
}
