# sda_versioning

### SDA Versioning Tool

This tool handles the versioning of the buildM instances in the context of DURAARK. 
The process is managed by keeping two graphs in the SDA, a temporary graph from which it reads the buildM instances;
the data is added from the DURAARK workbench into the temporary graph. 
Next, it lookups the buildM instances from the temporary graph whether they exist in the final graph, in case they do, it
adds a new version of the same instances and further links the buildM instance to the respective version. We make use of the
prov ontology to capture the different versions of a buildM instance. In particular, we use the predicate prov:wasRevisionOf,
where as a subject we have the buildM version (with URI pattern of http://data.duraark.eu/resource/[buildM-Identifier]/Version_[K] 
and as object the respective buildM instance.


The tool requires a config file which is given as an argument to the class SDOImport. 
In details, the tool is run through the class SDOImport and has the following arguments described below.

####Parameters
```operation``` - ifc_crawl_interlink | ifc_update | help
```config```` - the path to the config file
```resource``` - a filter when considering performing the versioning for a particular resource, when empty the versioning is
performed on all instances in the temporary graph.


####Configuration File
We have provided an example config file for that (versioning.config). These are the required attributes to be updated
```server``` - points to the server address where the SDA resides. A prerequisite is to be installed the ISQL, which comes 
together with the Virtuoso installation.
```user``` - the user name to the ISQL (should contain admin rights)
```password``` - the password for the provided ISQL user
```master_graph``` - the final graph where the SDA data resides. It has to exist in the SDA before any operation can be performed
by the versioning tool
```working_graph``` - the temporary graph, which stores that data as added by other processes through the DURAARK workbench
```endpoint``` - the URL to the SPARQL endpoint
```mysql_user``` - the mysql username, in case you want to have the functionallity of interlinking buildM instances with focused
crawling datasets
```mysql_password``` - the password to the mysql user name
```mysql_server``` - the address to the mysql server.
