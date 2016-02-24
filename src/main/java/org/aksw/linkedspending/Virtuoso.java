package org.aksw.linkedspending;

import static org.aksw.linkedspending.tools.PropertyLoader.*;
import java.sql.Statement;
import org.aksw.linkedspending.tools.PropertyLoader;
import lombok.SneakyThrows;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoUpdateFactory;

public class Virtuoso
{
	@SneakyThrows
	static public void createGraphGroup()
	{
		VirtGraph virtGraph = new VirtGraph(graph,jdbcUrl,jdbcUser,jdbcPassword);
		try(Statement statement = virtGraph.getConnection().createStatement())
		{
			VirtuosoUpdateFactory.create("CREATE SILENT GRAPH '"+PropertyLoader.graph+"'",virtGraph).exec();
			statement.execute("DB.DBA.RDF_GRAPH_GROUP_CREATE('"+PropertyLoader.graph+"',1)");
		}
		virtGraph.close();
	}

	@SneakyThrows
	static public void createSubGraph(String datasetName)
	{
		VirtGraph virtGraph = new VirtGraph(graph,jdbcUrl,jdbcUser,jdbcPassword);
		try(Statement statement = virtGraph.getConnection().createStatement())
		{
			String subGraph = PropertyLoader.graph+datasetName;

			VirtuosoUpdateFactory.create("CREATE SILENT GRAPH '"+subGraph+"'",virtGraph).exec();
			statement.execute("DB.DBA.RDF_GRAPH_GROUP_INS('"+PropertyLoader.graph+"','"+subGraph+"')");
		}
		virtGraph.close();
	}

	@SneakyThrows
	static public void deleteSubGraph(String datasetName)
	{
		VirtGraph virtGraph = new VirtGraph(graph,jdbcUrl,jdbcUser,jdbcPassword);
		try(Statement statement = virtGraph.getConnection().createStatement())
		{
			String subGraph = PropertyLoader.graph+datasetName;
			VirtuosoUpdateFactory.create("DROP SILENT GRAPH '"+subGraph+"'",virtGraph).exec();
			statement.execute("DB.DBA.RDF_GRAPH_GROUP_DEL('"+PropertyLoader.graph+"','"+subGraph+"')");
		}
		virtGraph.close();
	}

	/** Execute this once to create the graph group. */
	public static void main(String[] args)
	{
		createGraphGroup();
	}
}