package org.aksw.linkedspending;

import com.hp.hpl.jena.rdf.model.Model;
import org.aksw.linkedspending.tools.DataModel;
import org.junit.Test;

import java.io.*;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ConverterTest {
    /**
     * Copy muenster into the /json folder and muenster.dataset next to the .class file of this file to run the test
     */
    @Test
    public void testCreateDataset() {
        Set<String> datasetSet = new TreeSet<>();

        BufferedReader datasetReader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("muenster.dataset")));

        try {
            String line = null;
            while ((line = datasetReader.readLine()) != null) {
                datasetSet.add(line);
            }

            datasetReader.close();
        } catch (IOException e) {
            fail("Could not load example converted data: " + e);
        }

        ByteArrayOutputStream datasetOut =new ByteArrayOutputStream();
        ByteArrayOutputStream modelOut =new ByteArrayOutputStream();

        Model model = DataModel.newModel();
        try {
            Converter.createDataset("muenster", model, datasetOut);
        } catch (Exception e) {
            fail("Exception: " + e);
        }
        //model.write(modelOut);

        BufferedReader datasetIn = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(datasetOut.toByteArray())));
        try {
            String line = null;
            while ((line = datasetIn.readLine()) != null) {
                if(line.startsWith("<http://linkedspending.aksw.org/instance/observation-muenster-a8116617854a09b94aab14913ef8a3de37cf7d13> <http://dublincore.org/documents/2012/06/14/uri-terms/source> _:") ||
                   line.startsWith("<http://linkedspending.aksw.org/instance/observation-muenster-a80d78cf1b3afc7024560391dc958cf344c4f4ff> <http://dublincore.org/documents/2012/06/14/uri-terms/source> _:") ||
                   line.startsWith("<http://linkedspending.aksw.org/instance/muenster> <http://dublincore.org/documents/2012/06/14/uri-terms/created> \"")) {
                    // ignore generated string
                    // TODO: improve!!
                    continue;
                }
                if(datasetSet.contains(line)) {
                    datasetSet.remove(line);
                } else {
                    fail("Got data which is not in example data: " + line);
                }
            }
        } catch (IOException e) {
            fail("Exception while parsing output: " + e);
        }

        if(!datasetSet.isEmpty()) {
            fail("Some data was missing");
        }
    }
}

