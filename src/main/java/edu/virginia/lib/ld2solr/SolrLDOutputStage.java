/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.OutputStage;

/**
 * @author ajs6f
 * 
 */
public class SolrLDOutputStage implements OutputStage<SolrLDOutputStage> {

	private Acceptor<OutputRecord, ?> nextStage;

	@Override
	public void andThen(final Acceptor<OutputRecord, ?> s) {
		this.nextStage = s;
	}

	@Override
	public void accept(final NamedFields fields) {
		final SolrInputDocument record = SolrWrappedNamedFields.wrap.apply(fields);
		nextStage.accept(new OutputRecord() {

			@Override
			public String id() {
				return fields.id();
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
		});
	}
}
