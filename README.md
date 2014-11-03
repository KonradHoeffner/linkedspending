openspending2rdf
================

Usage
-----
Copy src/main/resources/credentials.properties.dist to ../credentials.properties and fill in your Virtuoso credentials.
Start org.aksw.linkedspending.Rest and access http://localhost:10010/datasets in your browser.
propertymapping.tsv is empty right now but can be filled with property mappings which differ from the automatic nameing scheme.

Development
-----------
Needs Maven.
Uses Project Lombok so you may have to integrate it into your GUI if you develop it with one, see http://projectlombok.org/features/index.html.

Code Style
----------
- see codeprofile.xml (Eclipse CodeFormatter profile)
- Java conventions: camel
- braces on the next line:
class X
{
	...
}

exception: only one line in a method:

void x() {x++;}

- functions, variables and methods lowerCamelCase, classes UpperCamelCase
- abbreviations in camelCase have only the first letter changed: Class SparqlQuery (not Class SPARQLQuery), String sparqlQuery (not String sPARQLQuery)
- /* short comments */ and /** javadocs **/ but no:
/*
comments with more space than necessary
*/

(should be "/*comments with more space than necessary*/")

- no licence or similar disclaimers
- usage of project annotations Lombok to save space
- usage of Eclipse @NonNull, @NonNullByDefault and @Nullable annotations
- JUnit tests for every nontrivial class
