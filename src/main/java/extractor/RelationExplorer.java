package extractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
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

    /**
     * initial node of starting graph exploration
     */
    private String startingNode;

    /**
     * maximum hops allowed
     */
    private int hops;

    /**
     * final node destination
     */
    private String endingNode;

    /**
     * collection of tasks to be executed
     */
    // Collection<Future<?>> futures = Collections
    // .synchronizedList(new LinkedList<Future<?>>());

    /**
     * collection of all visited nodes
     */
    static Vector<String> vectorOfQueryTerms = null;

    /**
     * flag to denote the end node is found
     */
    private static boolean ALARM;

    /**
     * inception point, starts from start node
     */
    private String ROOT = null;

    /**
     * graph that is incrementally formed
     */
    static DirectedGraph<String, DefaultEdge> graph = new SimpleDirectedGraph<String, DefaultEdge>(DefaultEdge.class);

    /**
     * Thread pool executor
     */
    private ExecutorService executor;

    int corePoolSize = 5;

    int maxPoolSize = 10;

    long keepAliveTime = 5000;

    private ThreadPoolExecutor threadPoolExecutor;

    // = new ThreadPoolExecutor(
    // corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS,
    // new LinkedBlockingQueue<Runnable>());

    /**
     * Constructor
     * 
     * @param dbSubj
     * @param dbObj
     * @param hops
     */
    public RelationExplorer(String dbSubj, String dbObj, int hops)
    {
        this.executor = Executors.newFixedThreadPool(20);

        this.threadPoolExecutor =
            new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

        this.threadPoolExecutor.allowCoreThreadTimeOut(true);

        this.startingNode = dbSubj;
        this.endingNode = dbObj;
        this.hops = hops;
    }

    /**
     * initial setup
     * 
     * @return path between. Empty if none found
     */
    public ArrayList<String> init()
    {

        ALARM = false;
        this.ROOT = this.startingNode;

        // clear up the vector for each initiation
        vectorOfQueryTerms = new Vector<String>();
        vectorOfQueryTerms.add(this.ROOT);
        
        // initiate the exploration
        doExploration(this.ROOT, this.endingNode);

        // wait for all other threads in the pool to finish
        while (this.threadPoolExecutor.getQueue().size() != 0 || this.threadPoolExecutor.getActiveCount() != 0) {
        }
        return findRelations(this.ROOT, this.endingNode);

    }

    public void kill()
    {

        this.threadPoolExecutor.shutdown();

        try {
            while (!this.executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)) {
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * exploration method. Recursive in nature where each blast spawns threads and recurses
     * 
     * @param startNode
     * @param endNode
     */
    public void doExploration(String startNode, final String endNode)
    {

        try {
            if (startNode.equals(endNode))
                return;

            if (!ALARM) {

                // add this as a graph node
                addVertexToGraph(startNode);

                // get the adjacent entity nodes
                List<QuerySolution> resultSet = getAdjacentNodes(startNode);

                // System.out.println(startNode + "\t" + resultSet.size());
                // iterate over all its vertices
                for (QuerySolution solution : resultSet) {

                    final String rel = solution.get("pred").toString();
                    final String obj = solution.get("obj").toString();

                    if (obj.indexOf("http://dbpedia.org/resource/") != -1
                        && obj.indexOf("http://dbpedia.org/resource/Category:") == -1 && !isInSet(obj)) {

                        addToList(obj);

                        // just for one triple, create this graph
                        addToGraph(startNode, rel, obj);

                        if (obj.equals(endNode)) {
                            ALARM = true;
                            return;
                        } else {

                            if (getShortestPathLength(obj) <= 2 * this.hops) {
                                // create workers for each result node
                                this.threadPoolExecutor.execute(new Runnable()
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

    /**
     * get the shortest path on the graph between the two nodes
     * 
     * @param obj
     * @return
     */
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

    /**
     * synchronized access to a list
     * 
     * @param item to be added
     */
    public static void addToList(String item)
    {
        synchronized (vectorOfQueryTerms) {
            if (!vectorOfQueryTerms.contains(item))
                vectorOfQueryTerms.add(item);
            vectorOfQueryTerms.notify();
        }
    }

    /**
     * synchronized check if an item is in the vector
     * 
     * @param item
     * @return true if contains
     */
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

    /**
     * a sparql endpoint query to fetch the adjacent objects
     * 
     * @param startNode
     * @return
     */
    private static List<QuerySolution> getAdjacentNodes(String startNode)
    {
        QueryExecution qexec = null;
        try {
            String sparqlQueryString =
                "select distinct ?pred ?obj where {<" + startNode
                    + "> ?pred ?obj. ?pred <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
                    + "<http://www.w3.org/2002/07/owl#ObjectProperty>} LIMIT 500";

            // DBpedia apparently doesn't allow repeated calls at a rate more
            // than some threshold requests/secd
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

            // get the edges between the nodes
            List<DefaultEdge> edges = DijkstraShortestPath.findPathBetween(graph, start, end);

            if (edges != null) {

                for (DefaultEdge edge : edges) {
                    String edgeVal = edge.toString().replaceAll("\\(", "").replaceAll("\\)", "");

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

        return listRels;
    }

    /**
     * synchronized addition to the graph
     * 
     * @param instance
     * @param rel
     * @param obj
     */
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

    /**
     * The starting point for the demo.
     * 
     * @param args ignored.
     * @return
     * @return
     */
    public static void main(String[] args)
    {

        // Check how many arguments were passed in
        if (args.length != 3) {
            System.out.println("Proper Usage is: java -jar pathFinder.jar <entity_1> <entity_2> <maximum_hops>");
            System.exit(0);
        } else {
            RelationExplorer relExp =
                new RelationExplorer("http://dbpedia.org/resource/" + args[0],
                    "http://dbpedia.org/resource/" + args[1], Integer.valueOf(args[2]));

            System.out.println(relExp.init());
        }
    }

}
