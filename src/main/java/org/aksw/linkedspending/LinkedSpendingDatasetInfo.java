package org.aksw.linkedspending;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.java.Log;
import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import org.aksw.linkedspending.tools.DataModel;
import org.aksw.linkedspending.tools.PropertyLoader;
import org.aksw.linkedspending.upload.UploadWorker;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

@Log
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@ToString
/** Represents information about RDF datasets on LinkedSpending and offers utility methods to collect it from the SPARQL endpoint.*/
public class LinkedSpendingDatasetInfo
{
	public final String name;
	public final Instant created;
	public final Instant modified;
	public final Instant sourceCreated;
	public final Instant sourceModified;
	public final int transformationVersion;

	private static Instant nodeToInstant(RDFNode n)
	{
		return Instant.parse(n.asLiteral().getLexicalForm());
	}

	/** fresh dataset information from SPARQL endpoint about a single dataset	 */
	public static Map<String,LinkedSpendingDatasetInfo> all()
	{
		Map<String,LinkedSpendingDatasetInfo> infos = new HashMap<>();
		String query = "select ?name ?c ?m ?sc ?sm ?tv {?d a qb:DataSet. ?d dcterms:identifier ?name. ?d dcterms:created ?c. ?d dcterms:modified ?m."
				+ "?d lso:sourceCreated ?sc. ?d lso:sourceModified ?sm. ?d lso:transformationVersion ?tv.}";
		ResultSet rs = Sparql.selectPrefixed(query);

		while(rs.hasNext())
		{
			QuerySolution qs = rs.next();
			String datasetName = qs.get("name").asLiteral().getLexicalForm();
			infos.put(datasetName,new LinkedSpendingDatasetInfo(datasetName, nodeToInstant(qs.get("c")), nodeToInstant(qs.get("m")),
					nodeToInstant(qs.get("sc")), nodeToInstant(qs.get("sm")),qs.get("tv").asLiteral().getInt()));

		}
		return infos;
	}

	/** fresh dataset information from SPARQL endpoint about a single dataset	 */
	public static Optional<LinkedSpendingDatasetInfo> forDataset(String datasetName)
	{
		String d = "<"+PropertyLoader.prefixInstance+datasetName+">";
		String query = "select ?c ?m ?sc ?sm ?tv {"+d+" dcterms:created ?c. "+d+" dcterms:modified ?m."
				+ ""+d+" lso:sourceCreated ?sc. "+d+" lso:sourceModified ?sm. "+d+" lso:transformationVersion ?tv}";
		ResultSet rs = Sparql.selectPrefixed(query);

		if(!rs.hasNext()) {return Optional.empty();}
		QuerySolution qs = rs.next();
		return Optional.of(new LinkedSpendingDatasetInfo(datasetName, nodeToInstant(qs.get("c")), nodeToInstant(qs.get("m")),
				nodeToInstant(qs.get("sc")), nodeToInstant(qs.get("sm")),qs.get("tv").asLiteral().getInt()));
	}

	// name is "primary key"
	@Override public int hashCode() {return name.hashCode();}

	@Override public boolean equals(Object obj)
	{
		if(!(obj instanceof LinkedSpendingDatasetInfo)) return false;
		return this.name.equals(((LinkedSpendingDatasetInfo)obj).name);
	}

	public static boolean isUpToDate(String datasetName)
	{
		Optional<LinkedSpendingDatasetInfo> lsInfo = forDataset(datasetName);
		if(!lsInfo.isPresent()) {return false;}
		if(lsInfo.get().transformationVersion<UploadWorker.TRANSFORMATION_VERSION) {return false;}
		OpenSpendingDatasetInfo osInfo;
		osInfo = OpenSpendingDatasetInfo.forDataset(datasetName);
		return lsInfo.get().modified.isAfter(osInfo.modified);
	}

}