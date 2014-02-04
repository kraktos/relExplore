package client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

/**
 * class to find relation between two nodes in a KB
 * 
 * @author Arnab Dutta
 */
public class Test
{
//
    private static double HOPS = 0;

    static Set<String> setQueryKeyWords = new HashSet<String>();

    private static boolean ALARM = false;

    static String ROOT = null;

    private static DijkstraShortestPath<String, DefaultEdge> path = null;

    /**
     * The starting point for the demo.
     * 
     * @param args ignored.
     */
    public static void main(String[] args)
    {

        ROOT = args[0];
        String endNode = args[1];// "http://dbpedia.org/resource/Euro";
        HOPS = 2 * Integer.parseInt(args[2]);

        DirectedGraph<String, DefaultEdge> g = new SimpleDirectedGraph<String, DefaultEdge>(DefaultEdge.class);

        setQueryKeyWords.add(ROOT);

        doQuery(ROOT, g, endNode);

        // System.out.println(g.toString());

        findRelations(g, ROOT, endNode);
    }

    public static void doQuery(String startNode, Graph<String, DefaultEdge> g, String endNode)
    {

        if (!ALARM) {

            g.addVertex(startNode);

            // System.out.println("Querying for = " + startNode);

            String sparqlQueryString1 =
                "select ?pred ?obj where {<"
                    + startNode
                    + "> ?pred ?obj. ?pred <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>}";

            Query query = QueryFactory.create(sparqlQueryString1);
            QueryExecution qexec = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", query);

            ResultSet results = qexec.execSelect();
            List<QuerySolution> e = ResultSetFormatter.toList(results);

            for (QuerySolution solution : e) {
                String rel = solution.get("pred").toString();
                String obj = solution.get("obj").toString();

                if (obj.indexOf("http://dbpedia.org/resource/") != -1
                    && obj.indexOf("http://dbpedia.org/resource/Category:") == -1 && !setQueryKeyWords.contains(obj)) {

                    // just for one triple, create this graph
                    addToGraph(startNode, g, rel, obj);

                    path = new DijkstraShortestPath<String, DefaultEdge>(g, ROOT, obj);

                    // System.out.println(path.getPathLength() + "\t" + ROOT
                    // + "\t" + obj);

                    if (path.getPathLength() > HOPS) {
                        return;
                    } else {
                        setQueryKeyWords.add(obj);

                        doQuery(obj, g, endNode);
                    }
                }

                if (ALARM)
                    return;

                if (obj.equals(endNode)) {
                    ALARM = true;
                    return;
                }

            }

            qexec.close();
        } else {
            return;
        }
    }

    /**
     * Graph is constructed.. now query for paths
     * 
     * @param g
     * @param start
     * @param end
     */
    private static void findRelations(DirectedGraph<String, DefaultEdge> g, String start, String end)
    {

        boolean flag = true;
        List<String> listRels = new ArrayList<String>();

        if (g.containsVertex(end)) {
            List<DefaultEdge> edges = DijkstraShortestPath.findPathBetween(g, start, end);

            // System.out.println(edges);
            for (DefaultEdge edge : edges) {
                String edgeVal = edge.toString().replaceAll("\\(", "").replaceAll("\\)", "");

                System.out.println(edgeVal);

                String[] strArr = edgeVal.split("\\s:\\s");
                if (flag) {
                    if (!listRels.contains(strArr[1]))
                        listRels.add(strArr[1]);
                    flag = false;
                } else {
                    // System.out.println(strArr[0]);
                    if (!listRels.contains(strArr[0]))
                        listRels.add(strArr[0]);
                    flag = true;
                }

            }
        }

        // System.out.println(g.toString());
        System.out.println(listRels);
    }

    private static void addToGraph(String instance, Graph<String, DefaultEdge> g, String rel, String obj)
    {
        g.addVertex(rel);
        g.addVertex(obj);
        g.addEdge(instance, rel);
        g.addEdge(rel, obj);
    }
}
