## :warning: **The LinkedSpending Conversion Tool is not maintained anymore and does not work with the current OpenSpending API.**
This repository has been archived on 2022-05-10.

LinkedSpending Data Conversion Tool
================


Download Data
-------------

* [Download LS package 2013-9 / 0.1 (~448 MB 7zip compressed, ~10 GB uncompressed)](https://github.com/KonradHoeffner/linkedspending/releases/download/data-2013-9/lscomplete20139.7z)
* [Download LS package 2014-3 (~2.1 GB 7zip compressed, ~25 GB uncompressed)](https://github.com/KonradHoeffner/linkedspending/releases/download/data-2014-3/lscomplete20143.7z)
* [Download LS package 2014-3 (~351 MB Brotli compressed, 1.3 GB uncompressed)](https://github.com/KonradHoeffner/linkedspending/releases/download/data-2014-3/lscomplete20143.hdt.br)
* [Download LS package 2014-3 (~475 MB 7gzip compressed, 1.3 GB uncompressed)](https://github.com/KonradHoeffner/linkedspending/releases/download/data-2014-3/lscomplete20143.hdt.gz)
* https://github.com/KonradHoeffner/linkedspending/releases/download/data-2014-3/lscomplete20143.hdt.br
* [Download the FinlandAid DataCube used by the CubeQA benchmark (~5.7 MB zip compressed, ~92 MB uncompressed)](https://github.com/KonradHoeffner/linkedspending/releases/download/data-finland-aid/finland-aid.nt.zip)
* [Download the 50 DataCubes used by the CubeQA qbench2 benchmark (~181 MB zip compressed, ~3.8 GB uncompressed)](https://github.com/KonradHoeffner/linkedspending/releases/download/data-qbench2datasets/qbench2datasets.zip)

See also
--------

* Semantic Web Journal Paper: [LinkedSpending: OpenSpending becomes Linked Open Data (2014)](http://www.semantic-web-journal.net/content/linkedspending-openspending-becomes-linked-open-data-1)
* PhD thesis including LinkedSpending: [Question Answering on RDF Data Cubes (2020)](https://nbn-resolving.org/urn:nbn:de:bsz:15-qucosa2-742429)
* Project web site: [GitHub repository linkedspending.aksw.org](https://github.com/konradhoeffner/linkedspending.aksw.org), online at <https://linkedspending.aksw.org/>
* [OpenSpending Blog Post about LinkedSpending](https://community.openspending.org/blog/2013/11/25/linkedspending-openspending-becomes-linked-open-data/)

Usage
-----
Copy `./src/main/resources/upload.properties.dist` to `./src/main/resources/upload.properties` and fill in your Virtuoso credentials.
Start `org.aksw.linkedspending.rest.Rest` and access <http://localhost:10010/datasets> in your browser.
`propertymapping.tsv` is empty right now but can be filled with property mappings which differ from the automatic naming scheme.

Development
-----------
Needs Maven.
Uses Java 8.
Using Java 9 may require creating the correct module info file, however the Maven configuration sets the compiler version to Java 8 so you should not run into this problem by accident but only if you purposely decide to upgrade Java to version 9.
Uses Project Lombok so you may have to integrate it into your GUI if you develop it with one, see <http://projectlombok.org/features/index.html>.

Code Style
----------
- see codeprofile.xml (Eclipse CodeFormatter profile)
- Java conventions: camel
- braces on the next line:

```java
class X
{
	...
}
```

exception: only one line in a method:

```java
void x() {x++;}
```

- functions, variables and methods lowerCamelCase, classes UpperCamelCase
- abbreviations in camelCase have only the first letter changed: `Class SparqlQuery` (not `Class SPARQLQuery`), `String sparqlQuery` (not String `sPARQLQuery`)
- `/* short comments */` and `/** javadocs **/` but no:
```java
/*
comments with more space than necessary
*/
```

(should be `/*comments with more space than necessary*/`)

- no licence or similar disclaimers
- usage of project annotations Lombok to save space
- usage of Eclipse `@NonNull`, `@NonNullByDefault` and `@Nullable` annotations
- JUnit tests for every nontrivial class

Known Issues
-------------
Some of the old scripts are not functional after the refactoring.
