/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.OutputStage;

/**
 * @author ajs6f
 * 
 */
public class SolrLDOutputStage implements OutputStage {

	public static final String ID_FIELD = "id";

	private final Iterator<ListenableFuture<NamedFields>> inputRecords;

	private final Function<NamedFields, SolrInputDocument> outputTransformation;

	public SolrLDOutputStage(final Iterator<ListenableFuture<NamedFields>> inputRecs,
			final Function<NamedFields, SolrInputDocument>... outputTransform) {
		// we allow for the possibility of a special transform from named fields
		// into a Solr document
		Function<NamedFields, SolrInputDocument> t = SolrWrappedNamedFields.wrap;
		if (outputTransform.length > 0) {
			t = outputTransform[0];
		}
		this.outputTransformation = t;
		this.inputRecords = inputRecs;
	}

	@Override
	public boolean hasNext() {
		return inputRecords.hasNext();
	}

	@Override
	public OutputRecord next() {
		try {
			final SolrInputDocument record = outputTransformation.apply(inputRecords.next().get());
			return new OutputRecord() {

				@Override
				public String id() {
					return (String) record.getFieldValue(ID_FIELD);
				}

				@Override
				public byte[] record() {
					try (StringWriter w = new StringWriter()) {
						new UpdateRequest().add(record).writeXML(w);
						return w.toString().getBytes();
					} catch (final IOException e) {
						throw propagate(e);
					}

				}
			};
		} catch (InterruptedException | ExecutionException e) {
			throw propagate(e);
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();

	}

}
