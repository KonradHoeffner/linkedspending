package org.aksw.linkedspending;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.aksw.linkedspending.tools.PropertyLoader;
import org.aksw.linkedspending.upload.UploadWorker;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
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

	static Map<String,LinkedSpendingDatasetInfo> lsInfoCache = new HashMap<>();
	static Instant lastCacheRefresh = Instant.MIN;
	static final Duration CACHE_TTL = Duration.ofHours(1);

	/** To improve performance and not stress the endpoint so much only a cached view is available that is updated periodically and for each dataset after its upload.
	 * Each modification of a dataset on the SPARQL endpoint needs to call updateCache on that dataset. */
	public synchronized static Map<String,LinkedSpendingDatasetInfo> cached()
	{
		if(lsInfoCache.isEmpty()||Duration.between(lastCacheRefresh, Instant.now()).compareTo(CACHE_TTL)>0)
		{
			lastCacheRefresh = Instant.now();
			lsInfoCache = all();
		}
		return lsInfoCache;
	}

	static public void updateCache(String datasetName)
	{
		// updates cache anyways so just fetch and throw away
		forDataset(datasetName);
	}

	/** fresh dataset information from SPARQL endpoint about a single dataset	 */
	private static Map<String,LinkedSpendingDatasetInfo> all()
	{
		Map<String,LinkedSpendingDatasetInfo> infos = new HashMap<>();
		String query = "select ?name ?c ?m ?sc ?sm ?tv {?d a qb:DataSet. ?d dcterms:identifier ?name. ?d dcterms:created ?c. ?d dcterms:modified ?m."
				+ "?d lso:sourceCreated ?sc. ?d lso:sourceModified ?sm. ?d lso:transformationVersion ?tv. ?d lso:uploadComplete \"true\"^^xsd:boolean.}";
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
				+ ""+d+" lso:sourceCreated ?sc. "+d+" lso:sourceModified ?sm. "+d+" lso:transformationVersion ?tv. "+d+" lso:uploadComplete \"true\"^^xsd:boolean.}";
		ResultSet rs = Sparql.selectPrefixed(query);

		if(!rs.hasNext()) {return Optional.empty();}
		QuerySolution qs = rs.next();
		LinkedSpendingDatasetInfo lsInfo = new LinkedSpendingDatasetInfo(datasetName, nodeToInstant(qs.get("c")), nodeToInstant(qs.get("m")),
				nodeToInstant(qs.get("sc")), nodeToInstant(qs.get("sm")),qs.get("tv").asLiteral().getInt());
		synchronized(lsInfoCache) {lsInfoCache.put(datasetName, lsInfo);}
		return Optional.of(lsInfo);
	}

	// name is "primary key"
	@Override public int hashCode() {return name.hashCode();}

	@Override public boolean equals(Object obj)
	{
		if(!(obj instanceof LinkedSpendingDatasetInfo)) return false;
		return this.name.equals(((LinkedSpendingDatasetInfo)obj).name);
	}

	public static boolean newestTransformation(String datasetName)
	{
		Optional<LinkedSpendingDatasetInfo> lsInfo = forDataset(datasetName);
		if(!lsInfo.isPresent()) {return false;}
		return lsInfo.get().transformationVersion>=UploadWorker.TRANSFORMATION_VERSION;
	}


	public static boolean upToDate(String datasetName)
	{
		Optional<LinkedSpendingDatasetInfo> lsInfo = forDataset(datasetName);
		if(!lsInfo.isPresent()) {return false;}
		OpenSpendingDatasetInfo osInfo;
		osInfo = OpenSpendingDatasetInfo.forDataset(datasetName);
		return lsInfo.get().modified.isAfter(osInfo.modified);
	}

}