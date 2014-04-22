package org.aksw.linkedspending;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.LogManager;
import lombok.NonNull;
import lombok.extern.java.Log;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.aksw.linkedspending.tools.DataModel;
import org.aksw.linkedspending.tools.PropertiesLoader;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;
import de.konradhoeffner.commons.MemoryBenchmark;
import de.konradhoeffner.commons.Pair;
import de.konradhoeffner.commons.TSVReader;
import org.aksw.linkedspending.tools.Exceptions.*;

// bug? berlin_de doesnt have any measure (should at least have "amount") on sparql endpoint 

/** The main application which does the conversion. Requires JSONDownloader.main() to be called first in order for the input files to exist.
 * Output consists of ntriples files in the chosen folder.**/
@NonNullByDefault
@Log
@SuppressWarnings("serial")
public class Main
{
    /** properties */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");

    static MemoryBenchmark memoryBenchmark = new MemoryBenchmark();
    static ObjectMapper m = new ObjectMapper();
    static final boolean USE_CACHE = Boolean.parseBoolean(PROPERTIES.getProperty("useCache", "true"));

    static List<String> faultyDatasets = new LinkedList<>();

    static File folder = new File("output20143");
    static File statistics = new File("statistics"+(System.currentTimeMillis()/1000));

    //    static final boolean CACHING = true;
    static {        
        if(USE_CACHE) {CacheManager.getInstance().addCacheIfAbsent("openspending-json");}
    }
    static final Cache cache = USE_CACHE?CacheManager.getInstance().getCache("openspending-json"):null;

    static final Map<String,String> codeToCurrency = new HashMap<>();
    static
    {
        try(TSVReader in = new TSVReader(Main.class.getClassLoader().getResourceAsStream("codetocurrency.tsv")))
        {
            while(in.hasNextTokens())
            {
                String[] tokens = in.nextTokens();
                codeToCurrency.put(tokens[0], tokens[1]);
            }
        }
        catch (Exception e) {throw new RuntimeException(e);}
    }

    static final Map<Pair<String>,String> datasetPropertyNameToUri = new HashMap<>();
    static
    {
        try(TSVReader in = new TSVReader(Main.class.getClassLoader().getResourceAsStream("propertymapping.tsv")))
        {
            while(in.hasNextTokens())
            {
                String[] tokens = in.nextTokens();
                datasetPropertyNameToUri.put(new Pair<String>(tokens[0], tokens[1]),tokens[2]);
            }
        }
        catch (Exception e) {throw new RuntimeException(e);}
    }

    @Nullable static String cleanString(@Nullable String s)
    {
        if(s==null||"null".equals(s)||s.trim().isEmpty()) return null;
        return s;
    }

    /** Creates component specifications. Adds backlinks from their parent DataStructureDefinition.
     * @throws UnknownMappingTypeException */
    static Set<ComponentProperty> createComponents(JsonNode mapping, Model model,String datasetName, Resource dataset, Resource dsd, boolean datasetHasYear) throws MalformedURLException, IOException, MissingDataException, UnknownMappingTypeException
    {
        int attributeCount = 1; // currency is always there and dataset is not created if it is not found
        int dimensionCount = 0;
        int measureCount = 0;
        Map<String,Property> componentPropertyByName = new HashMap<>();
        Set<ComponentProperty> componentProperties = new HashSet<>();
        //        ArrayNode dimensionArray = readArrayNode(url);        
        boolean dateExists = false;
        for(Iterator<String> it = mapping.fieldNames();it.hasNext();)
        {
            //            JsonNode dimJson = dimensionArray.get(i);
            String key = it.next();
            JsonNode componentJson = mapping.get(key);

            //            String name = cleanString(componentJson.get("name"));

            String name = key;
            @NonNull String type = cleanString(componentJson.get("type").asText());
            assert type!=null;
            @Nullable String label = cleanString(componentJson.get("label").asText());

            //            String componentPropertyUrl = componentJson.get("html_url");
            String componentPropertyUrl;

            String uri = datasetPropertyNameToUri.get(new Pair<String>(datasetName,name));
            componentPropertyUrl=(uri!=null)?uri: DataModel.LSOntology.getUri() + name;

            Property componentProperty = model.createProperty(componentPropertyUrl);                
            componentPropertyByName.put(name, componentProperty);

            Resource componentSpecification = model.createResource(componentPropertyUrl+"-spec");            
            model.add(componentSpecification, RDFS.label, "specification of "+label);

            model.add(componentProperty, RDF.type, RDF.Property);

            if(label!=null) {model.add(componentProperty,RDFS.label,label);}
            else
            {
                label = name;
                if(label!=null) {model.add(componentProperty,RDFS.label,label);}
            }

            if(componentJson.has("description"))
            {
                String description = cleanString(componentJson.get("description").asText());
                if(description!=null) {model.add(componentProperty,RDFS.comment,description);}
            } else {log.warning("no description for "+key);}

            switch(type)
            {
                case "date":
                {
                    dateExists = true;
                    dimensionCount++;
                    componentSpecification = DataModel.LSOntology.getDateComponentSpecification();
                    componentProperties.add(new ComponentProperty(DataModel.LSOntology.getRefDate(),name,ComponentProperty.Type.DATE));
                    // it's a dimension
                    //                    model.add(componentSpecification, DataCube.dimension, componentProperty);
                    //                    model.add(componentProperty, RDF.type, DataCube.DimensionProperty);

                    //                    model.add(componentProperty, RDFS.subPropertyOf,SDMXDIMENSION.refPeriod);
                    //                    componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.DATE));

                    // concept
                    //                    model.add(componentProperty, DataCube.concept,SDMXCONCEPT.timePeriod);
                    //                        if()
                    //                        model.add(dim, RDFS.range,XmlSchema.gYear);
                    break;
                }
                case "compound":
                {
                    dimensionCount++;
                    // it's a dimension
                    model.add(componentSpecification, DataModel.DataCube.getDimension(), componentProperty);
                    model.add(componentSpecification, RDF.type, DataModel.DataCube.getComponentSpecification());
                    model.add(componentProperty, RDF.type, DataModel.DataCube.getDimensionProperty());
                    //                        assertTrue(); TODO: assert that the "attributes" of the json are always "name" and "label"
                    componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.COMPOUND));
                    //TODO: model.add(componentProperty, DataCube.concept,SDMXCONCEPT. ???);
                    break;
                }
                case "measure":
                {
                    measureCount++;
                    model.add(componentSpecification, DataModel.DataCube.getMeasure(), componentProperty);
                    model.add(componentSpecification, RDF.type, DataModel.DataCube.getComponentSpecification());
                    model.add(componentProperty, RDF.type, DataModel.DataCube.getMeasureProperty());

                    componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.MEASURE));
                    //TODO: model.add(componentProperty, DataCube.concept,SDMXCONCEPT. ???);
                    break;
                }
                case "attribute":
                {
                    attributeCount++;
                    // TODO: attribute the same meaning as in DataCube?
                    model.add(componentSpecification, DataModel.DataCube.getAttribute(), componentProperty);
                    model.add(componentSpecification, RDF.type, DataModel.DataCube.getComponentSpecification());
                    model.add(componentProperty, RDF.type, DataModel.DataCube.getAttributeProperty());

                    componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.ATTRIBUTE));
                    //TODO: model.add(componentProperty, DataCube.concept,SDMXCONCEPT. ???);
                    break;
                }
                default: throw new UnknownMappingTypeException("unkown type: "+type+"of mapping element "+componentJson);
            }
            // backlink
            model.add(dsd, DataModel.DataCube.getComponent(), componentSpecification);
        }
        //        if(dateExists||datasetHasYear)
        //        {
        //
        //        }
        //if(!dateExists) {throw new MissingDataException("No date for dataset "+dataset.getLocalName());}
        if(attributeCount==0||measureCount==0||dimensionCount==0)
        {throw new MissingDataException("no "+(attributeCount==0?"attributes":(measureCount==0?"measures":"dimensions"))+" for dataset "+dataset.getLocalName());}
        return componentProperties;
    }

    //    static void createViews(URL url, Model model) throws MalformedURLException, IOException
    //    {
    //        ArrayNode views = readArrayNode(new URL(url+".json"));
    //        for(int i=0;i<views.length();i++)
    //        {
    //            JsonNode view = views.get(i);
    //
    //            String name = view.get("name");
    //            String label = view.get("label");
    //            String description = view.get("description");
    //
    //            JsonNode state = view.get("state");
    //            String year = state.get("year"); // TODO: what to do with the year?
    //            Integer.valueOf(year); // throws an exception if its not an integer
    //
    //            JsonNode cuts = state.get("cuts");
    //            List<String> drilldowns = ArrayNodeToStringList(state.getArrayNode("drilldowns"));
    //
    //            Resource slice = model.createResource(url+"/slice/"+name);
    //            {
    //                model.add(slice,RDF.type,DataCube.Slice);
    //                //            model.add(slice,DataCube.sliceStructure,...); // TODO
    //                //                model.add(slice,DataCube.sliceStructure,...);
    //                model.add(slice,RDFS.label,model.createLiteral(label));
    //                model.add(slice,RDFS.comment,model.createLiteral(description));
    //            }
    //            {
    //                Resource sliceKey = model.createResource(url+"/"+name);
    //                model.add(sliceKey,RDF.type,DataCube.SliceKey);
    //                model.add(sliceKey,RDFS.label,model.createLiteral(label));
    //                model.add(sliceKey,RDFS.comment,model.createLiteral(description));
    //                for(Iterator<String> it = cuts.keys(); it.hasNext();)
    //                {
    //                    String dimensionName = it.next();
    //                    String dimensionValue = cuts.get(dimensionName);
    //                    model.add(sliceKey,DataCube.componentProperty,componentPropertyByName.get(dimensionName));
    //                    model.add(slice,componentPropertyByName.get(dimensionName),dimensionValue);
    //                }
    //
    //            }
    //
    //
    //        }
    //    }

    static void createCodeLists(ArrayNode views, Model model) throws MalformedURLException, IOException
    {
        //        for(int i=0;i<views.length();i++)
        //        {
        //        }
    }

    //    static void createViews(URL datasetUrl, ArrayNode views, Model model,Map<String,Property> componentPropertyByName) throws MalformedURLException, IOException
    //    {
    //        for(int i=0;i<views.length();i++)
    //        {
    //            JsonNode view = views.get(i);
    //            String entity = view.get("entity");
    //            String name = view.get("name");
    //            String drilldownName = view.get("drilldown");
    //            Property drilldownProperty = componentPropertyByName.get("drilldown");
    //
    //            String label = view.get("label");
    //            String description = view.get("description");
    //
    //            switch(entity)
    //            {
    //                case "dataset":
    //                {
    //                    // create the code list
    //                    Resource codeList = model.createResource(datasetUrl+"/codelists/"+name);
    //                    model.add(codeList,RDF.type,DataCube.HierarchicalCodeList);
    //                    model.add(codeList,RDFS.label,model.createLiteral("code list for property "+drilldownName,"en"));
    //                    codeListByName.put(drilldownName, codeList);
    //                    break;
    //                }
    //                case "dimension":
    //                {
    //                    Property dimension = componentPropertyByName.get("dimension");
    //                    model.add(codeListByName.get(dimension),DataCube.parentChildProperty,drilldownProperty);
    //                    break;
    //                }
    //                default: throw new RuntimeException("unknown entity value: "+entity+" for view "+view);
    //            }
    //
    //            JsonNode state = view.get("state");
    //            String year = state.get("year"); // TODO: what to do with the year?
    //            Integer.valueOf(year); // throws an exception if its not an integer
    //
    //            JsonNode cuts = state.get("cuts");
    //            List<String> drilldowns = ArrayNodeToStringList(state.getArrayNode("drilldowns"));
    //
    //            //            Resource slice = model.createResource(datasetUrl+"/slice/"+name);
    //            //            {
    //            //                model.add(slice,RDF.type,DataCube.Slice);
    //            //                //            model.add(slice,DataCube.sliceStructure,...); // TODO
    //            //                //                model.add(slice,DataCube.sliceStructure,...);
    //            //                model.add(slice,RDFS.label,model.createLiteral(label));
    //            //                model.add(slice,RDFS.comment,model.createLiteral(description));
    //            //            }
    //            //            {
    //            //                Resource sliceKey = model.createResource(datasetUrl+"/slicekey/"+name);
    //            //                model.add(sliceKey,RDF.type,DataCube.SliceKey);
    //            //                model.add(sliceKey,RDFS.label,model.createLiteral(label));
    //            //                model.add(sliceKey,RDFS.comment,model.createLiteral(description));
    //            //                for(Iterator<String> it = cuts.keys(); it.hasNext();)
    //            //                {
    //            //                    String dimensionName = it.next();
    //            //                    String dimensionValue = cuts.get(dimensionName);
    //            //                    model.add(sliceKey,DataCube.componentProperty,componentPropertyByName.get(dimensionName));
    //            //                    model.add(slice,componentPropertyByName.get(dimensionName),dimensionValue);
    //            //                }
    //            //
    //            //            }
    //
    //
    //        }
    //    }
    /**reads JSON-file from json folder and generates observations
     * writes observations to output folder
     * writes statistics
     * deletes the model! @param url entries url, e.g. http://openspending.org/berlin_de/entries.json     (TODO: or http://openspending.org/api/2/search?dataset=berlin_de&format=json ?)
     * @param componentProperties the dimensions which are expected to be values for in all entries.
     * @param countries
     * @param datasetName the JSON-file to be read from */

    static void createObservations(String datasetName,Model model,OutputStream out, Resource dataSet, Set<ComponentProperty> componentProperties,@Nullable Resource currency, Set<Resource> countries,@Nullable Literal yearLiteral)
            throws MalformedURLException, IOException, TooManyMissingValuesException
            {
        JsonDownloader.ResultsReader in = new JsonDownloader.ResultsReader(datasetName);
        JsonNode result;
        boolean dateExists = false;
        Set<Integer> years = new HashSet<Integer>();
        int missingValues = 0;
        int expectedValues = 0;
        Map<ComponentProperty,Integer> missingForProperty = new HashMap<>();
        int observations;
        for(observations=0;(result=in.read())!=null;observations++)
        {
            String osUri = result.get("html_url").asText();
            Resource osObservation = model.createResource();
            String suffix = osUri.substring(osUri.lastIndexOf('/')+1);
            String lsUri = PROPERTIES.getProperty("urlInstance") + "observation-"+datasetName+"-"+suffix;
            Resource observation = model.createResource(lsUri);
            model.add(observation, RDFS.label, datasetName+"// TODO Auto-generated method stub, observation "+suffix);
            model.add(observation, DataModel.DataCube.getDataSet(), dataSet);
            model.add(observation, RDF.type, DataModel.DataCube.getObservation());
            model.add(observation, DataModel.DCMI.source,osObservation);
            //            boolean dateExists=false;
            for(ComponentProperty d: componentProperties)
            {
                //                if(d.name==null) {throw new RuntimeException("no name for component property "+d);}
                expectedValues++;
                if(!result.has(d.name))
                {
                    Integer missing = missingForProperty.get(d);
                    missing = (missing==null)?1:missing+1;
                    missingForProperty.put(d,missing);
                    missingValues++;
                    int minMissing = Integer.parseInt(PROPERTIES.getProperty("minValuesMissingForStop"));
                    int maxMissing = Integer.parseInt(PROPERTIES.getProperty("maxValuesMissingLogged"));
                    float missingStopRatio = Float.parseFloat(PROPERTIES.getProperty("datasetMissingStopRatio"));
                    if(missingForProperty.get(d)<=maxMissing) {log.warning("no entry for property "+d.name+" at entry "+result);}
                    if(missingForProperty.get(d)==maxMissing) {log.warning("more missing entries for property "+d.name+".");}
                    if(missingValues>=minMissing&&((double)missingValues/expectedValues>=missingStopRatio)) {faultyDatasets.add(datasetName);throw new TooManyMissingValuesException(datasetName,missingValues);}
                    continue;
                }
                try
                {
                    switch(d.type)
                    {
                        case COMPOUND:
                        {
                            JsonNode jsonDim = result.get(d.name);
                            //                            if(jsonDim==null)
                            //                            {
                            //                                errors++;
                            //                                log.warning("no url for entry "+d.name);
                            //                                continue;
                            //                            }
                            if(!jsonDim.has("html_url"))
                            {
                                log.warning("no url for "+jsonDim);
                                missingValues++;
                                continue;
                            }
                            JsonNode urlNode = jsonDim.get("html_url");
                            // todo enhancement: interlinking auf dem label -> besser extern
                            // todo enhancement: ressource nicht mehrfach erzeugen - aber aufpassen dass der speicher nicht voll wird! wird wohl nur im datenset gehen

                            Resource instance = model.createResource(urlNode.asText());

                            if(jsonDim.has("label")) {model.addLiteral(instance,RDFS.label,model.createLiteral(jsonDim.get("label").asText()));}
                            else    {log.warning("no label for dimension "+d.name+" instance "+instance);}
                            model.add(observation,d.property,instance);

                            break;
                        }
                        case ATTRIBUTE:
                        {
                            String s = result.get(d.name).asText();
                            model.addLiteral(observation,d.property,model.createLiteral(s));
                            break;
                        }
                        case MEASURE:
                        {
                            String s = result.get(d.name).asText();
                            Literal l;
                            try {l = model.createTypedLiteral(Integer.parseInt(s));}
                            catch(NumberFormatException e)
                            {l=model.createLiteral(s);}
                            model.addLiteral(observation,d.property,l);
                            break;
                        }
                        case DATE:
                        {
                            dateExists=true;
                            JsonNode jsonDate = result.get(d.name);
                            //                            String week = date.get("week");
                            int year = jsonDate.get("year").asInt();
                            int month = jsonDate.get("month").asInt();
                            int day = jsonDate.get("day").asInt();
                            model.addLiteral(observation, DataModel.LSOntology.getRefDate(),model.createTypedLiteral(year+"-"+month+"-"+day, XSD.date.getURI()));
                            model.addLiteral(observation, DataModel.LSOntology.getRefYear(),model.createTypedLiteral(year, XSD.gYear.getURI()));
                            years.add(year);
                        }
                    }

                }
                catch(Exception e)
                {
                    throw new RuntimeException("problem with componentproperty "+d.name+": "+observation,e);
                }
            }
            //
            //            String label = result.get("label");
            //
            //            if(label!=null&&!label.equals("null")) {model.add(observation,RDFS.label,label);}
            //            else
            //            {
            //                label = result.get("name");
            //                if(label!=null&&!label.equals("null")) {model.add(observation,RDFS.label,label);}
            //            }
            //            String description = result.get("description");
            //            if(description!=null&&!description.equals("null")) {model.add(observation,RDFS.comment,description);}
            //
            //            String type = result.get("type");
            //            switch(type)
            //            {
            //                case "date":
            //                {
            //                    model.add(observation, RDFS.subPropertyOf,SDMXDIMENSION.refPeriod);
            //                    //                        if()
            //                    //                        model.add(dim, RDFS.range,XmlSchema.gYear);
            //                    return;
            //                }
            //                case "compound":return;
            //                case "measure":return;
            //                case "attribute":return;
            //            }

            if(currency!=null)
            {
                model.add(observation, DataModel.DBPOntology.currency, currency);
            }

            if(yearLiteral!=null&&!dateExists) // fallback, in case entry doesnt have a date attached we use year of the whole dataset
            {
                model.addLiteral(observation, DataModel.LSOntology.getRefYear(),yearLiteral);
            }
            for(Resource country: countries)
            {
                // add the countries to the observations as well (not just the dataset)
                model.add(observation, DataModel.SdmxAttribute.getRefArea(),country);
            }
            if(model.size()>Integer.parseInt(PROPERTIES.getProperty("maxModelTriples")))
            {
                log.fine("writing triples");
                writeModel(model,out);
            }
        }
        // completeness statistics
        if(expectedValues==0)
        {
            log.warning("no observations for dataset "+datasetName+".");
        } else
        {
            model.addLiteral(dataSet, DataModel.LSOntology.getCompleteness(), 1-(double)(missingValues/expectedValues));
            for(ComponentProperty d: componentProperties)
            {
                model.addLiteral(d.property, DataModel.LSOntology.getCompleteness(), 1-(double)(missingValues/expectedValues));
            }

            // in case the dataset goes over several years or doesnt have a default time attached we want all the years of the observations on the dataset
            for(int year: years)
            {
                model.addLiteral(dataSet, DataModel.LSOntology.getRefYear(),model.createTypedLiteral(year, XSD.gYear.getURI()));
            }
            writeModel(model,out);
            // write missing statistics
            try(PrintWriter statisticsOut  = new PrintWriter(new BufferedWriter(new FileWriter(statistics, true))))
            {
                if(missingForProperty.isEmpty()) {statisticsOut.println("no missing values");}
                else {statisticsOut.println(datasetName+'\t'+((double)missingValues/observations)+'\t'+(double)Collections.max(missingForProperty.values())/observations);}
            }
        }
        log.info(observations+" observations created.");
            }

    static void writeModel(Model model, OutputStream out)
    {
        model.write(out,"N-TRIPLE");
        //        model.write(out,"TURTLE");
        // assuming that most memory is consumed before model cleaning
        memoryBenchmark.updateAndGetMaxMemoryBytes();
        model.removeAll();
    }

    /** Takes a json url of an openspending dataset model and extracts rdf into a jena model.
     * The DataStructureDefinition (DSD) specifies the structure of a dataset and contains a set of qb:ComponentSpecification resources.
     * @param url json url that contains an openspending dataset model, e.g. http://openspending.org/fukuoka_2013/model
     * @param model initialized model that the triples will be added to
     */
    static Resource createDataStructureDefinition(final URL url,Model model) throws MalformedURLException, IOException
    {
        log.finer("Creating DSD");
        Resource dsd = model.createResource(url.toString());
        model.add(dsd, RDF.type, DataModel.DataCube.getDataStructureDefinition());
        //        JsonNode dsdJson = readJSON(url);
        // mapping is now gotten in createdataset
        //        JsonNode mapping = dsdJson.get("mapping");
        //        for(Iterator<String> it = mapping.keys();it.hasNext();)
        //        {
        //            String key = it.next();
        //            JsonNode dimJson = mapping.get(key);
        //            String type = dimJson.get("type");
        //            switch(type)
        //            {
        //                case "compound":return;
        //                case "measure":return;
        //                case "date":return;
        //                case "attribute":return;
        //            }

        //            if(1==1)throw new RuntimeException(dimURL);
        //            Resource dim = model.createResource(dimURL);
        //            model.add(dim,RDF.type,DataCube.DimensionProperty);

        //            String label = dimJson.get("label");
        //            if(label!=null&&!label.equals("null")) {model.add(dim,RDFS.label,label);}
        //            String description = dimJson.get("description");
        //            if(description!=null&&!description.equals("null")) {model.add(dim,RDFS.comment,description);}


        //            System.out.println(dimJson);
        //        }

        //        if(dsdJson.has("views"))
        //        {
        //            ArrayNode views = dsdJson.getArrayNode("views");
        //        }

        //        System.out.println("Converting dataset "+url);
        return dsd;
    }

    static void deleteDataset(String datasetName)
    {
        System.out.println("******************************++deelte"+datasetName);
        Converter.getDatasetFile(datasetName).delete();
    }

    /** Takes the url of an openspending dataset and extracts rdf into a jena model.
     * Each dataset contains a model which gets translated to a datastructure definition and entries that contain the actual measurements and get translated to a
     * DataCube.
     * @param model initialized model that the triples will be added to
     * @throws IOException
     * @throws NoCurrencyFoundForCodeException
     * @throws DatasetHasNoCurrencyException
     * @throws UnknownMappingTypeException
     * @throws TooManyMissingValuesException
     * @returns if it was successfully created
     */
    static void createDataset(String datasetName,Model model,OutputStream out)
            throws IOException, NoCurrencyFoundForCodeException, DatasetHasNoCurrencyException, MissingDataException, UnknownMappingTypeException, TooManyMissingValuesException
            {
        @NonNull URL url = new URL(PROPERTIES.getProperty("urlInstance") + datasetName);
        @NonNull URL sourceUrl = new URL(PROPERTIES.getProperty("urlOpenSpending") + datasetName+".json");
        @NonNull JsonNode datasetJson = readJSON(sourceUrl);
        @NonNull Resource dataSet = model.createResource(url.toString());
        @NonNull Resource dsd = createDataStructureDefinition(new URL(url+"/model"), model);
        model.add(dataSet, DataModel.DCMI.source,model.createResource(PROPERTIES.getProperty("urlOpenSpending") + datasetName));
        model.add(dataSet, DataModel.DCMI.created,model.createTypedLiteral(GregorianCalendar.getInstance()));

        // currency is defined on the dataset level in openspending but in RDF datacube we decided to define it for each observation
        Resource currency = null;

        if(datasetJson.has("currency"))
        {
            String currencyCode = datasetJson.get("currency").asText();
            currency = model.createResource(codeToCurrency.get(currencyCode));
            if(currency == null) {throw new NoCurrencyFoundForCodeException(datasetName,currencyCode);}
            model.add(dsd, DataModel.DataCube.getComponent(), DataModel.LSOntology.getCurrencyComponentSpecification());

            //            model.add(currencyComponent, DataCube.attribute, SdmxAttribute.currency);
            //            model.addLiteral(SdmxAttribute.currency, RDFS.label,model.createLiteral("currency"));
            //            model.add(SdmxAttribute.currency, RDF.type, RDF.Property);
            //            model.add(SdmxAttribute.currency, RDF.type, DataCube.AttributeProperty);
            //            //model.add(SdmxAttribute.currency, RDFS.subPropertyOf,SDMXMEASURE.obsValue);
            //            model.add(SdmxAttribute.currency, RDFS.range,XSD.decimal);
        } else {log.warning("no currency for dataset "+datasetName+", skipping");throw new DatasetHasNoCurrencyException(datasetName);}
        final Integer defaultYear;
        {
            String defaultYearString = cleanString(datasetJson.get("default_time").asText());
            // we only want the year, not date and time which are 1-1 and 0:0:0 anyways
            if(defaultYearString!=null) defaultYearString = defaultYearString.substring(0, 4);
            defaultYear = defaultYearString==null?null:Integer.valueOf(defaultYearString);
        }
        Set<ComponentProperty> componentProperties;
        try {componentProperties = createComponents(readJSON(new URL(PROPERTIES.getProperty("urlOpenSpending") + datasetName+"/model")).get("mapping"), model,datasetName,dataSet, dsd,defaultYear!=null);    }
        catch (MissingDataException | UnknownMappingTypeException e)
        {
            log.severe("Error creating components for dataset "+datasetName);
            throw e;
        }

        model.add(dataSet, RDF.type, DataModel.DataCube.getDataSetResource());
        model.add(dataSet, DataModel.DataCube.getStructure(), dsd);
        String dataSetName = url.toString().substring(url.toString().lastIndexOf('/')+1);

        List<String> territories = ArrayNodeToStringList((ArrayNode)datasetJson.get("territories"));
        Set<Resource> countries = new HashSet<>();
        @Nullable Literal yearLiteral = null;
        if(defaultYear!=null)
        {
            model.add(dsd, DataModel.DataCube.getComponent(), DataModel.LSOntology.getYearComponentSpecification());
            yearLiteral = model.createTypedLiteral(defaultYear, XSD.gYear.getURI());
            model.add(dataSet, DataModel.LSOntology.getRefYear(),yearLiteral);
        }
        if(!territories.isEmpty())
        {
            model.add(dsd, DataModel.DataCube.getComponent(), DataModel.LSOntology.getCountryComponent());
            for(String territory: territories)
            {
                Resource country = model.createResource(Countries.lgdCountryByCode.get(territory));
                countries.add(country);
                model.add(dataSet, DataModel.SdmxAttribute.getRefArea(),country);
            }
        }
        {
            //        JsonNode entries = readJSON(new URL("http://openspending.org/api/2/search?format=json&pagesize="+MAX_ENTRIES+"&dataset="+dataSetName),true);
            //        log.fine("extracting results");
            //        ArrayNode results = (ArrayNode)entries.get("results");
            log.fine("creating entries");

            createObservations(datasetName, model, out, dataSet, componentProperties, currency, countries, yearLiteral);
            log.fine("finished creating entries");
        }
        createViews(datasetName,model,dataSet);
        List<String> languages = ArrayNodeToStringList((ArrayNode)datasetJson.get("languages"));

        //         qb:component [qb:attribute sdmx-attribute:unitMeasure;
        //         qb:componentRequired "true"^^xsd:boolean;
        //         qb:componentAttachment qb:DataSet;]
        String label = datasetJson.get("label").asText();
        String description = datasetJson.get("description").asText();

        // doesnt work well enough
        //        // guess the language for the language tag
        //        // we assume that label and description have the same language
        //        Detector detector = DetectorFactory.create();
        //        detector.append(label);
        //        detector.append(description);
        //        String language = detector.detect();
        //        model.add(dataSet, RDFS.label, label,language);
        //        model.add(dataSet, RDFS.comment, description,language);
        model.add(dataSet, RDFS.label, label);
        model.add(dataSet, RDFS.comment, description);
        // todo: find out the language
        //        model.createStatement(arg0, arg1, arg2)
        //        System.out.println("Converting dataset "+url);
            }

    public static void createViews(String datasetName,Model model, Resource dataSet) throws MalformedURLException, IOException
    {
        ArrayNode views = readArrayNode(new URL(PROPERTIES.getProperty("urlOpenSpending") + datasetName+"/views.json"));
        for(int i=0;i<views.size();i++)
        {
            JsonNode jsonView = views.get(i);
            String name = jsonView.get("name").asText();
            Resource view = model.createResource(PROPERTIES.getProperty("urlInstance") + datasetName+"/views/"+name);
            model.add(view,RDF.type, DataModel.DataCube.getSliceResource());
            model.add(dataSet, DataModel.DataCube.getSlice(),view);
            String label = jsonView.get("label").asText();
            String description = jsonView.get("description").asText();
            model.add(view, RDFS.label, label);
            model.add(view, RDFS.comment, description);
        }
    }

    public static String readJSONString(URL url) throws IOException {return readJSONString(url,false,USE_CACHE);}
    public static String readJSONString(URL url,boolean detailedLogging) throws IOException {return readJSONString(url,false,USE_CACHE);}
    public static String readJSONString(URL url,boolean detailedLogging,boolean USE_CACHE) throws IOException    
    {
        //        System.out.println(cache.getKeys());
        if(USE_CACHE)
        {
            Element e = cache.get(url.toString());
            if(e!=null) {/*System.out.println("cache hit for "+url.toString());*/return (String)e.getObjectValue();}
        }
        if(detailedLogging) {log.fine("cache miss for "+url.toString());}        

        // SWP 14 team: here is a start for the response code handling which you should get to work, I discontinued it because the connection
        // may be a non-httpurlconnection (if the url relates to a file) so maybe there should be two readJsonString methods, one for a file and one for an http url
        // or maybe it should be split into two methods where this one only gets a string as an input and the error handling for connections should be somewhere else
        // of course there shouldn't be System.out.println() statements, they are just placeholders.
        // error handling isnt even that critical here but needs to be in any case in the JSON downloader for the big parts 
        //        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        //        connection.connect();
        //        int response = connection.getResponseCode(); 
        //        switch(response)
        //        {
        //            case HttpURLConnection.HTTP_OK: System.out.println("OK"); // fine, continue            
        //            case HttpURLConnection.HTTP_GATEWAY_TIMEOUT: System.out.println("gateway timeout"); // retry
        //            case HttpURLConnection.HTTP_UNAVAILABLE: System.out.println("unavailable"); // abort
        //            default: log.error("unhandled http response code "+response+". Aborting download of dataset."); // abort
        //        }
        //        try(Scanner undelimited = new Scanner(connection.getInputStream(), "UTF-8"))
        try(Scanner undelimited = new Scanner(url.openStream(), "UTF-8"))
        {
            try(Scanner scanner = undelimited.useDelimiter("\\A"))
            {
                String datasetsJsonString = scanner.next();
                char firstChar = datasetsJsonString.charAt(0);
                if(!(firstChar=='{'||firstChar=='[')) {throw new IOException("JSON String for URL "+url+" seems to be invalid.");}
                if(USE_CACHE) {cache.put(new Element(url.toString(), datasetsJsonString));}
                //IfAbsent            
                return datasetsJsonString;            
            }
        }
    }

    public static JsonNode readJSON(URL url,boolean detailedLogging) throws JsonProcessingException, IOException
    {
        String content = readJSONString(url,detailedLogging);
        if(detailedLogging) {log.fine("finished loading text, creating json object from text");}
        return m.readTree(content);
        //        try {return new JsonNode(readJSONString(url));}
        //        catch(JSONException e) {throw new IOException("Could not create a JSON object from string "+readJSONString(url),e);}
    }

    public static JsonNode readJSON(URL url) throws IOException
    {
        return readJSON(url,false);
    }

    public static ArrayNode readArrayNode(URL url) throws IOException    
    {
        return (ArrayNode)m.readTree(readJSONString(url));
        //        try {return new ArrayNode(readJSONString(url));}
        //        catch(JSONException e) {throw new IOException("Could not create a JSON array from string "+readJSONString(url),e);}
    }

    public static List<String> ArrayNodeToStringList(ArrayNode ja)
    {
        List<String> l = new LinkedList<>();
        for(int i=0;i<ja.size();i++)
        {
            l.add(ja.get(i).asText());
        }
        return l;
    }

    static void shutdown(int status)
    {
        if(USE_CACHE) {CacheManager.getInstance().shutdown();}
        System.exit(status);
    }


}