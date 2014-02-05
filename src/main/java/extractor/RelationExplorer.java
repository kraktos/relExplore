package extractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jgrapht.DirectedGraph;
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
public class RelationExplorer
{

    static Vector<String> vectorOfQueryTerms = new Vector<String>();

    private static boolean ALARM;

    private String ROOT = null;

    static DirectedGraph<String, DefaultEdge> graph = new SimpleDirectedGraph<String, DefaultEdge>(DefaultEdge.class);

    private ExecutorService executor;

    private String staringNode;

    private int hops;

    private String endingNode;

    public RelationExplorer(String dbSubj, String dbObj, int hops)
    {
        this.executor = Executors.newFixedThreadPool(300);

        this.staringNode = dbSubj;
        this.endingNode = dbObj;
        this.hops = hops;
    }

    public ArrayList<String> init()
    {
        try {
            ALARM = false;
            this.ROOT = this.staringNode;

            vectorOfQueryTerms.add(this.ROOT);

            doExploration(this.ROOT, this.endingNode);

            return findRelations(this.ROOT, this.endingNode);
        } finally {
            this.kill();
        }

    }

    public void kill()
    {

        this.executor.shutdown();

        try {
            while (!this.executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)) {
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * The starting point for the demo.
     * 
     * @param args ignored.
     * @return
     * @return
     */
    public static void main(String[] args)
    {

        RelationExplorer relExp = new RelationExplorer("", "", 2);

        relExp.init();
        relExp.kill();

    }

    public void doExploration(final String startNode, final String endNode)
    {

        try {
            if (!ALARM) {

                addVertexToGraph(startNode);

                // get the adjacent entuty nodes
                List<QuerySolution> resultSet = getAdjacentNodes(startNode);

                for (QuerySolution solution : resultSet) {

                    String rel = solution.get("pred").toString();
                    final String obj = solution.get("obj").toString();

                    // System.out.println(rel + "\t" + obj);

                    if (obj.indexOf("http://dbpedia.org/resource/") != -1
                        && obj.indexOf("http://dbpedia.org/resource/Category:") == -1 && !isInSet(obj)) {

                        addToList(obj);

                        // just for one triple, create this graph
                        addToGraph(startNode, rel, obj);

                        if (obj.equals(endNode)) {
                            ALARM = true;
                            return;
                        } else {

                            DijkstraShortestPath<String, DefaultEdge> path =
                                new DijkstraShortestPath<String, DefaultEdge>(graph, this.ROOT, obj);

                            if (getShortestPathLength(obj) <= this.hops) {
                                // create workers for each result node
                                this.executor.execute(new Runnable()
                                {
                                    public void run()
                                    {
                                        doExploration(obj, endNode);
                                    }
                                });
                            } else
                                return;

                        }
                    }

                    if (ALARM)
                        return;

                }
            } else
                return;
        } catch (Exception e) {

            e.printStackTrace();
        }

    }

    private double getShortestPathLength(String obj)
    {
        synchronized (graph) {
            DijkstraShortestPath<String, DefaultEdge> path =
                new DijkstraShortestPath<String, DefaultEdge>(graph, this.ROOT, obj);

            return path.getPathLength();
        }
    }

    private static void delay(int delay)
    {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * @param startNode
     */
    public static void addVertexToGraph(final String node)
    {
        synchronized (graph) {
            graph.addVertex(node);
            graph.notify();
        }
    }

    public static void addToList(String item)
    {
        // synchronized (setQueryKeyWords) {
        // setQueryKeyWords.add(item);
        // setQueryKeyWords.notify();
        // }

        synchronized (vectorOfQueryTerms) {
            if (!vectorOfQueryTerms.contains(item))
                vectorOfQueryTerms.add(item);
            vectorOfQueryTerms.notify();
        }
    }

    public static boolean isInSet(String item)
    {
        synchronized (vectorOfQueryTerms) {

            if (vectorOfQueryTerms.contains(item)) {
                // setQueryKeyWords.notify();
                return true;
            } else {
                // setQueryKeyWords.notify();
                return false;
            }
        }
    }

    private static List<QuerySolution> getAdjacentNodes(String startNode)
    {
        QueryExecution qexec = null;
        try {
            String sparqlQueryString =
                "select distinct ?pred ?obj where {<" + startNode
                    + "> ?pred ?obj. ?pred <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
                    + "<http://www.w3.org/2002/07/owl#ObjectProperty>} LIMIT 500";

            delay(300);

            Query query = QueryFactory.create(sparqlQueryString);
            qexec = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", query);
            ResultSet results = qexec.execSelect();

            return ResultSetFormatter.toList(results);
        } finally {
            qexec.close();
        }

    }

    /**
     * Graph is constructed.. now query for paths
     * 
     * @param g
     * @param start
     * @param end
     * @return
     */
    private static ArrayList<String> findRelations(String start, String end)
    {

        boolean flag = true;
        ArrayList<String> listRels = new ArrayList<String>();

        if (graph.containsVertex(end)) {
            List<DefaultEdge> edges = DijkstraShortestPath.findPathBetween(graph, start, end);

            if (edges != null) {
                // System.out.println(edges);
                for (DefaultEdge edge : edges) {
                    String edgeVal = edge.toString().replaceAll("\\(", "").replaceAll("\\)", "");

                    // System.out.println(edgeVal);

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
        }

        // System.out.println(g.toString());
        return listRels;
    }

    private static void addToGraph(String instance, String rel, String obj)
    {
        synchronized (graph) {

            graph.addVertex(rel);
            graph.addVertex(obj);

            graph.addEdge(instance, rel);
            graph.addEdge(rel, obj);

            graph.notify();
        }
    }
}
