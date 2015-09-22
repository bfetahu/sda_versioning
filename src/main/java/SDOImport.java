import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.util.FileManager;
import utils.FileUtils;
import virtuoso.jena.driver.VirtModel;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 8/31/15.
 */
public class SDOImport {
    public static void main(String[] args) throws SQLException {
        String operation = "", config = "", resource = "";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-operation")) {
                operation = args[++i];
            } else if (args[i].equals("-config")) {
                config = args[++i];
            } else if (args[i].equals("-resource")) {
                resource = args[++i];
            }
        }

        //interlink the IFC file with existing crawls.
        if (operation.equals("ifc_crawl_interlink")) {
            //search in the crawl configuration database for crawls that are related to a particular instance
            interlinkBuildMInstanceWithCrawls(resource, config);
        } else if (operation.equals("ifc_update")) {
            //update an existing IFC file such that we add another version to the existing data graph
            updateBuildMVersion(config, resource);
        } else if (operation.equals("help")) {
            StringBuffer sb = new StringBuffer();
            sb.append("\n=============== SDA Import/Update =====================\n");
            sb.append("-operation\t Takes two possible values: ifc_crawl_interlink and ifc_update.").
                    append("\n\t\t (a) ifc_crawl_interlink - Loads all existing IFC instances in the given SDA and interlinks them to existing crawls.").
                    append("\n\t\t (b) ifc_update - Loads the IFC instances that are in the working graph in SDA and loads them into the final SDA graph. \n\t\t\tIf there is already an exisitng version, then a new one is appended via the triple ?instance buildM:hasVersion ?version").
                    append("\n\n Parameters: (1) sda_config, (2) resource, (3) mysql_config.\n").
                    append("\n\t\t\t: (1) config: Should hold as key value pairs (tab delimited) the following attributes for configuring the SDA endpoint:").
                    append("\n\t\t\t\t{endpoint\tendpoint_url}, {working_graph\tworking_graph_uri}, {master_graph\tmaster_graph_uri}, {server\tserver_uri}, {user\tuser}, {password\tpassowrd}").
                    append("\n\t\t\t\t{mysql_server\tserver_uri}, {mysql_database\tdatabase_name}, {mysql_user\tuser}, {mysql_password\tpassowrd}").
                    append("\n\t\t\t: (2) resource: the resource URI for which you want to filter all the mentioned operations.\n");
            System.out.println(sb.toString());
        }
    }

    public static void updateBuildMVersion(String sda_config, String resource) {
        //load the virtuoso endpoint configuration
        Map<String, String> sda_config_properties = FileUtils.readIntoStringMap(sda_config, "\t", false);

        //load the buildM instances that need to be updated.
        Map<String, Map<String, Set<String>>> instances_to_update = loadbuildMInstances(resource, sda_config_properties.get("endpoint"), sda_config_properties.get("working_graph"));

        //load the existing versions
        Map<String, Integer> buildM_versions = loadbuildMVersions(resource, sda_config_properties.get("endpoint"), sda_config_properties.get("master_graph"));

        //for each buildM instance create a proxy resource with the given URI and add the subsequent versions
        String out_file = "buildM_instances_to_update.nt";
        for (String resource_uri : instances_to_update.keySet()) {
            int max_version = buildM_versions.containsKey(resource_uri) ? buildM_versions.get(resource_uri) + 1 : 1;
            String resource_name = resource_uri.substring(resource_uri.lastIndexOf("/") + 1, resource_uri.lastIndexOf(">"));
            String buildM_subversion = "<http://data.duraark.eu/resource/version/" + resource_name + "/" + max_version + ">";
            StringBuffer sb = new StringBuffer();

            sb.append(buildM_subversion).append("\t<http://www.w3.org/TR/prov-o/#wasRevisionOf>\t").append(resource_uri).append(".\n");
            for (String predicate : instances_to_update.get(resource_uri).keySet()) {
                for (String object_value : instances_to_update.get(resource_uri).get(predicate)) {
                    sb.append(buildM_subversion).append("\t").append(predicate).append("\t").append(object_value).append(". \n");
                }
            }

            FileUtils.saveText(sb.toString(), out_file, true);
        }

        //write the data into the graph.
        Model model = VirtModel.openDatabaseModel(sda_config_properties.get("master_graph"), "jdbc:virtuoso://" + sda_config_properties.get("server") + ":1111", sda_config_properties.get("user"), sda_config_properties.get("password"));
        InputStream in = FileManager.get().open(out_file);
        if (in == null) {
            throw new IllegalArgumentException("File: " + out_file + " not found");
        }
        model.read(new InputStreamReader(in), null, "N-TRIPLE");
        model.close();
        File f = new File(out_file);
        f.delete();
    }

    /**
     * Interlinks a given buildM instance with existing crawls. In case the resource is empty, first it fetches
     * all buildM instances and matches them with a given crawl.
     *
     * @param resource
     */
    public static void interlinkBuildMInstanceWithCrawls(String resource, String sda_config) throws SQLException {
        //load the virtuoso endpoint configuration
        Map<String, String> sda_config_properties = FileUtils.readIntoStringMap(sda_config, "\t", false);
        //load the buildM instances
        Map<String, Set<String>> data = loadbuildMLocations(resource, sda_config_properties.get("endpoint"), sda_config_properties.get("master_graph"));
        Map<String, Set<String>> crawls = loadFinishedCrawls(sda_config_properties);

        //for each instance check if you can find a crawl.
        StringBuffer sb = new StringBuffer();
        for (String buildm_instance : data.keySet()) {
            Set<String> locations = data.get(buildm_instance);

            for (String crawl_uri : crawls.keySet()) {
                Set<String> seeds = crawls.get(crawl_uri);

                //check the overlap between the locations and the seed lists.
                Set<String> tmp = new HashSet<String>(seeds);
                tmp.retainAll(locations);

                //if there is an overlap interlink the buildM instance with the crawl.
                if (!tmp.isEmpty()) {
                    sb.append(buildm_instance).append("\t<http://www.w3.org/2000/01/rdf-schema#seeAlso>\t").append(crawl_uri).append(" .\n");
                }
            }
        }

        String out_file = "crawl_interlinking.nt";
        FileUtils.saveText(sb.toString(), out_file);

        //write the data into the graph.
        Model model = VirtModel.openDatabaseModel(sda_config_properties.get("master_graph"), "jdbc:virtuoso://" + sda_config_properties.get("server") + ":1111", sda_config_properties.get("user"), sda_config_properties.get("password"));
        InputStream in = FileManager.get().open(out_file);
        if (in == null) {
            throw new IllegalArgumentException("File: " + out_file + " not found");
        }
        model.read(new InputStreamReader(in), null, "N-TRIPLE");
        model.close();
        File f = new File(out_file);
        f.delete();
    }


    /**
     * Loads the set of buildM instances that match the resource URI (in case this is given). As a result we get the
     * resources, and their address locality, such that we can match them to the existing crawls.
     *
     * @param resource
     * @param endpoint
     * @return
     */
    public static Map<String, Set<String>> loadbuildMLocations(String resource, String endpoint, String master_graph) {
        Map<String, Set<String>> data = new HashMap<String, Set<String>>();
        try {
            String querystr = "";
            if (!resource.isEmpty()) {
                querystr = "SELECT ?resource ?location WHERE { GRAPH <" + master_graph + "> {?resource <http://data.duraark.eu/vocab/buildm/addressLocality> ?location. FILTER (regex(?resource, \"" + resource + "\", \"i\"))}}";
            } else {
                querystr = "SELECT ?resource ?location WHERE { GRAPH <" + master_graph + "> {?resource <http://data.duraark.eu/vocab/buildm/addressLocality> ?location}}";
            }
            QueryEngineHTTP query = new QueryEngineHTTP(endpoint, querystr);

            ResultSet rst = query.execSelect();
            while (rst.hasNext()) {
                QuerySolution qs = rst.next();
                RDFNode resource_node = qs.get("?resource");
                RDFNode location_node = qs.get("?location");

                String resource_uri = "<" + resource_node.asResource().getURI() + ">";
                String location = location_node.isResource() ? "<" + location_node.asResource().getURI() + ">" : "\"" + location_node.asLiteral().getString() + "\"";

                if (location.contains("/")) {
                    location = location.substring(location.lastIndexOf("/") + 1).trim();
                }

                if (!data.containsKey(resource_uri)) {
                    data.put(resource_uri, new HashSet<String>());
                }
                data.get(resource_uri).add(location);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return data;
    }

    /**
     * Loads the set of buildM instances that match the resource URI (in case this is given). As a result we get the
     * resources, and their address locality, such that we can match them to the existing crawls.
     *
     * @param resource
     * @param endpoint
     * @return
     */
    public static Map<String, Integer> loadbuildMVersions(String resource, String endpoint, String master_graph) {
        Map<String, Integer> data = new HashMap<String, Integer>();
        try {
            String querystr = "";
            if (!resource.isEmpty()) {
                querystr = "SELECT ?resource ?version  WHERE { GRAPH <" + master_graph + "> {?resource <http://data.duraark.eu/vocab/buildM/hasVersion> ?version. FILTER (regex(?resource, \"" + resource + "\", \"i\"))}}";
            } else {
                querystr = "SELECT ?resource ?version  WHERE { GRAPH <" + master_graph + "> {?resource <http://data.duraark.eu/vocab/buildM/hasVersion> ?version}}";
            }
            QueryEngineHTTP query = new QueryEngineHTTP(endpoint, querystr);
            ResultSet rst = query.execSelect();

            while (rst.hasNext()) {
                QuerySolution qs = rst.next();
                RDFNode resource_node = qs.get("?resource");
                RDFNode version_node = qs.get("?version");

                String resource_uri = "<" + resource_node.asResource().getURI() + ">";
                String version_uri = version_node.asResource().getURI();
                version_uri = version_uri.substring(version_uri.lastIndexOf("/") + 1);

                int version = Integer.valueOf(version_uri);
                if (!data.containsKey(resource_uri)) {
                    data.put(resource_uri, version);
                } else if (data.get(resource_uri) < version) {
                    data.put(resource_uri, version);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return data;
    }

    /**
     * Loads the set of buildM instances that match the resource URI (in case this is given).
     * We get the full description of the resource.
     *
     * @param resource
     * @param endpoint
     * @return
     */
    public static Map<String, Map<String, Set<String>>> loadbuildMInstances(String resource, String endpoint, String working_graph) {
        Map<String, Map<String, Set<String>>> data = new HashMap<String, Map<String, Set<String>>>();
        try {
            String querystr = "";
            if (!resource.isEmpty()) {
                querystr = "SELECT ?resource ?predicate ?object WHERE { GRAPH <" + working_graph + "> {?resource ?predicate ?object. FILTER (regex(?resource, \"" + resource + "\", \"i\"))}}";
            } else {
                querystr = "SELECT ?resource ?predicate ?object WHERE { GRAPH <" + working_graph + "> {?resource ?predicate ?object}}";
            }
            QueryEngineHTTP query = new QueryEngineHTTP(endpoint, querystr);

            ResultSet rst = query.execSelect();
            while (rst.hasNext()) {
                QuerySolution qs = rst.next();
                RDFNode resource_node = qs.get("?resource");
                RDFNode predicate_node = qs.get("?predicate");
                RDFNode object_node = qs.get("?object");

                String resource_uri = "<" + resource_node.asResource().getURI() + ">";
                String predicate = "<" + predicate_node.toString() + ">";
                String object = object_node.isResource() ? "<" + object_node.asResource().getURI() + ">" : "\"" + object_node.asLiteral().getString() + "\"";

                if (!data.containsKey(resource_uri)) {
                    data.put(resource_uri, new HashMap<String, Set<String>>());
                }

                if (!data.get(resource_uri).containsKey(predicate)) {
                    data.get(resource_uri).put(predicate, new HashSet<String>());
                }
                data.get(resource_uri).get(predicate).add(object);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return data;
    }

    /**
     * Loads all existing crawls from the database.
     *
     * @return
     */
    public static Map<String, Set<String>> loadFinishedCrawls(Map<String, String> mysql_params) throws SQLException {
        Map<String, Set<String>> data = new HashMap<String, Set<String>>();
        Connection conn = getMySQLConnection(mysql_params.get("mysql_user"), mysql_params.get("mysql_password"), mysql_params.get("mysql_database"), mysql_params.get("mysql_server"));

        PreparedStatement prep = conn.prepareStatement("SELECT crawl_id, seed_list FROM crawl_configurations WHERE end_timestamp IS NOT NULL");
        java.sql.ResultSet rst = prep.executeQuery();
        while (rst.next()) {
            int crawl_id = rst.getInt("crawl_id");
            String[] seeds = rst.getString("seed_list").split(";");

            String crawl_uri = "<http://data.duraark.eu/crawl/" + crawl_id + ">";
            Set<String> sub_data = new HashSet<String>();
            data.put(crawl_uri, sub_data);
            for (String seed : seeds) {
                String seed_tmp = seed;
                if (seed.indexOf("/") != -1) {
                    seed_tmp = seed.substring(seed.lastIndexOf("/") + 1).trim();
                }
                sub_data.add(seed_tmp);
            }
        }

        return data;
    }


    private static Connection conn;

    public static Connection getMySQLConnection(String user, String password, String database, String server) {
        if (conn == null) {
            String dbURL = "jdbc:mysql://" + server + ":3306/" + database + "?useUnicode=true&characterEncoding=utf-8";

            try {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection(dbURL, user, password);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return conn;
        }
        return conn;
    }
}
