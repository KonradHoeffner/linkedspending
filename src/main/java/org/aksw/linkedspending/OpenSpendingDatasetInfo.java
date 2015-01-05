package org.aksw.linkedspending;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.ToString;
import lombok.extern.java.Log;
import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import org.aksw.linkedspending.tools.PropertyLoader;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/** Represents information about source datasets from OpenSpending and offers utility methods to collect it from the OpenSpending JSON API.*/
@Log
@ToString
public class OpenSpendingDatasetInfo
{
	public final String name;
	public final Instant created;
	public final Instant modified;

	private OpenSpendingDatasetInfo(String name, Instant created, Instant modified)
	{
		this.name = name;
		this.created = created;
		this.modified = modified;
	}

	private static final File DATASETS_CACHED	 = new File("cache/datasets.json");
	private static final ObjectMapper				m				= new ObjectMapper();
	//	private static final Set<String>	unfinishedDatasets	= new HashSet<>();
	//	private static final Set<String>	finishedDatasets	= new HashSet<>();
	private static final Set<String>	emptyDatasets	= Collections.synchronizedSet(new HashSet<String>());
	private static final File emptyDatasetFile	= new File("cache/emptydatasets.ser");

	static protected final SortedMap<String,OpenSpendingDatasetInfo> datasetInfos = new TreeMap<String, OpenSpendingDatasetInfo>();

	static Instant lastCacheRefresh = Instant.ofEpochMilli(0);
	static private long CACHE_TTL_MINUTES = 15;

	static private Object readLock = new Object();

	public static Lock onlineLock = new ReentrantLock();
	public static Condition onlineCondition = onlineLock.newCondition();

	static boolean isOnline = isOnline();
	static Instant lastOnlineCheck  = Instant.now();
	static final long ONLINE_CHECK_INTERVAL_MINUTES = 10;

	public static boolean isOnline()
	{
		onlineLock.lock();
		try
		{
			if(Duration.between(lastOnlineCheck, Instant.now())
					.compareTo(Duration.ofMinutes(ONLINE_CHECK_INTERVAL_MINUTES))<0)
			{
				return isOnline;
			}
			lastOnlineCheck = Instant.now();
			m.readTree(new URL("https://openspending.org/berlin_de/entries.json?pagesize=0"));
			return true;
		}
		catch (IOException e) {return false;}
		finally
		{
			if(isOnline) {onlineCondition.signalAll();}
			onlineLock.unlock();
		}
	}
	/**
	 *
	 * loads the names of datasets(JSON-files) <br>
	 * if #datasetNames already exists, return them<br>
	 * if cache-file exists, load datasets from cache-file<br>
	 * if cache-file does not exist, load from openspending and write cache-file
	 * @return a set containing the names of all JSON-files
	 * @throws IOException
	 * - if one of many files can't be read from or written to
	 * @see JsonDownloader.getDatasetNamesFresh() */



	public static SortedMap<String,OpenSpendingDatasetInfo> getDatasetInfosCached()
	{
		synchronized(readLock)
		{
			return new TreeMap<>(getDatasetInfos(true));
		}
	}

	/** get fresh dataset names from openspending and update the cash.
	 * @see JsonDownloader.getDatasetNamesCached()*/
	public static SortedMap<String,OpenSpendingDatasetInfo> getDatasetInfosFresh()
	{
		synchronized(readLock)
		{
			return new TreeMap<>(getDatasetInfos(false));
		}
	}

	// todo does the cache file get updated once in a while? if not functionality is needed
	/** @param readCache read datasets from cache (may be outdated but faster) */
	private static SortedMap<String,OpenSpendingDatasetInfo> getDatasetInfos(boolean readCache)
	{
		try
		{
			JsonNode datasets = null;
			boolean fresh = false;
			if(readCache&&(Duration.between(lastCacheRefresh, Instant.now()).compareTo(Duration.of(CACHE_TTL_MINUTES, ChronoUnit.MINUTES))<0))
			{
				if (!datasetInfos.isEmpty()) return datasetInfos;
				if(DATASETS_CACHED.exists()) {	datasets = m.readTree(DATASETS_CACHED);}
			} else
			{
				datasetInfos.clear();
			}
			// either caching didn't work or it is disabled
			if(datasets==null)
			{
				lastCacheRefresh = Instant.now();
				datasets = m.readTree(PropertyLoader.urlDatasets);
				fresh = true;
				m.writeTree(new JsonFactory().createGenerator(DATASETS_CACHED, JsonEncoding.UTF8), datasets);
			}

			ArrayNode datasetArray = (ArrayNode) datasets.get("datasets");
			if(fresh) {log.info("Fetched datasets from OpenSpending ("+Duration.between(lastCacheRefresh, Instant.now())+"): "+datasetArray.size() + " datasets available. ");}
			// emptyDatasets.size() + " marked as empty, "	+ (datasetArray.size() - emptyDatasets.size()) + " remaining.");
			for (int i = 0; i < datasetArray.size(); i++)
			{
				JsonNode datasetJson = datasetArray.get(i);
				String name  = datasetJson.get("name").textValue();
				datasetInfos.put(name,new OpenSpendingDatasetInfo(
						name,
						Instant.parse(datasetJson.get("timestamps").get("created").asText()+'Z'),
						Instant.parse(datasetJson.get("timestamps").get("last_modified").asText()+'Z')));
			}
			return datasetInfos;
		}
		catch(IOException e) {throw new RuntimeException(e);}
	}

	/** fresh dataset information from OpenSpending about a single dataset
	 * @param datasetName the identifier of the dataset, such as "2013" or "berlin_de"
	 * @return a representation of the dataset meta data
	 * @throws DataSetDoesNotExistException in case the dataset does not exist in OpenSpending.
	 * This results in an exception while it doesn't for LinkedSpendingDatasetInfo
	 * because a dataset from the OpenSpending dataset list should always exist at OpenSpending. */
	public static OpenSpendingDatasetInfo forDataset(String datasetName) throws DataSetDoesNotExistException
	{
		URL url = null;
		try
		{
			url = new URL(PropertyLoader.prefixOpenSpending+datasetName+".json");
			JsonNode datasetJson = m.readTree(url);
			return new OpenSpendingDatasetInfo(datasetName,
					Instant.parse(datasetJson.get("timestamps").get("created").asText()+'Z'),
					Instant.parse(datasetJson.get("timestamps").get("last_modified").asText()+'Z'));
		}
		catch (IOException e)
		{
			if(e.getMessage().contains("HTTP response code 404")) {throw new DataSetDoesNotExistException(datasetName);}
			throw new RuntimeException("Error getting OpenSpending dataset info for dataset '"+datasetName+"' at url "+url,e);
		}
	}


}