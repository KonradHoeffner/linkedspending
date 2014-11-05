package org.aksw.linkedspending;

import java.time.Instant;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.java.Log;
import org.aksw.linkedspending.tools.PropertiesLoader;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

@Log
@AllArgsConstructor(access = AccessLevel.PRIVATE)
/** Represents information about RDF datasets on LinkedSpending and offers utility methods to collect it from the SPARQL endpoint.*/
public class LinkedSpendingDatasetInfo
{
	public final String name;
	public final Instant created;
	public final Instant modified;
	public final Instant sourceCreated;
	public final Instant sourceModified;

	private static Instant nodeToInstant(RDFNode n)
	{
		return Instant.parse(n.asLiteral().getLexicalForm());
	}

	/** fresh dataset information from OpenSpending about a single dataset	 */
	public static Optional<LinkedSpendingDatasetInfo> forDataset(String datasetName)
	{
			String d = "<"+PropertiesLoader.prefixInstance+datasetName+">";
			ResultSet rs = Sparql.select("select ?c ?m ?sc ?sm {"+d+" dcterms:created ?c. "+d+" dcterms:modified ?modified."
					+ ""+d+" lso:sourceCreated ?sc. "+d+" lso:sourceModified ?sm}");
			if(!rs.hasNext()) {return Optional.empty();}
			QuerySolution qs = rs.next();
			return Optional.of(new LinkedSpendingDatasetInfo(datasetName, nodeToInstant(qs.get("c")), nodeToInstant(qs.get("m")),
					nodeToInstant(qs.get("sc")), nodeToInstant(qs.get("sm"))));
	}


}