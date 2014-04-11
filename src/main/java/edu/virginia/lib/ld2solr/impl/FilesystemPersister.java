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
public class FilesystemPersister implements RecordPersister {

	File directory;

	private static final Logger log = getLogger(FilesystemPersister.class);

	/**
	 * @param directory
	 */
	public FilesystemPersister(final File d) {
		if (!d.isDirectory()) {
			throw new IllegalArgumentException("Filesystem location must be a directory!");
		}
		this.directory = d;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.virginia.lib.ld2solr.spi.RecordSink#accept(edu.virginia.lib.ld2solr
	 * .api.OutputRecord)
	 */
	@Override
	public void accept(final OutputRecord record) throws IOException {
		final File file = new File(directory, record.id());
		log.debug("Persisting to: {}", file.toString());
		Files.asByteSink(file).write(record.record());
	}

	/**
	 * @return the directory to which we are writing
	 */
	public File directory() {
		return directory;
	}

}
