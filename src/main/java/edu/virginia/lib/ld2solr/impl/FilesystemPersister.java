package edu.virginia.lib.ld2solr.impl;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.slf4j.Logger;

import com.google.common.io.Files;

import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.RecordSink.RecordPersister;
import edu.virginia.lib.ld2solr.spi.ThreadedStage;

/**
 * A {@link RecordPersister} that writes {@link OutputRecord}s to a filesystem.
 * 
 * @author ajs6f
 * 
 */
public class FilesystemPersister extends ThreadedStage<FilesystemPersister, OutputRecord> implements RecordPersister {

	private File directory;

	private static final Logger log = getLogger(FilesystemPersister.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see RecordSink#accept(OutputRecord)
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
	 * @see RecordSink.RecordPersister#location(java.lang.String)
	 */
	@Override
	public FilesystemPersister location(final String location) {
		directory = new File(location);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see RecordSink.RecordPersister#location()
	 */
	@Override
	public String location() {
		return directory.getAbsolutePath();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ThreadedStage#andThen(edu.virginia.lib.ld2solr.spi.Stage.Acceptor)
	 */
	@Override
	public <A extends Acceptor<OutputRecord, ?>> A andThen(final A a) {
		throw new UnsupportedOperationException();
	}

}
