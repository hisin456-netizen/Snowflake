Created 25-NOV-2003

Updated 30-JAN-2023

========================================================================

Copyright (c) 2023 Oracle and/or its affiliates. All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

========================================================================

1. Introduction

The Oracle Database Documentation
(https://docs.oracle.com/en/database/oracle/oracle-database/index.html) and
other resources use these sample schemas to demonstrate the capabilities of
Oracle Database (https://www.oracle.com/database/) and the SQL and PL/SQL
languages. You can download and install these schemas in your database
environments to learn about Oracle Database functionality or try the examples
in the documentation. The sample schemas are based on a fictitious sample
company that sells goods through various channels. The company operates
worldwide to fill orders for products. It has several divisions, each of which
is represented by a sample database schema.

These schemas are provided for free.

1.1 About the Sample Schemas

The sample database schemas provide a common platform for examples in each
release of Oracle Database.

The following schemas are included:

- HR: Human Resources - useful for introducing basic topics.
- CO: Customer Orders - a modern schema useful for demos of e-commerce
                        transactions. It allows the storage of semi-structured
                        data using JSON.
- SH: Sales History - designed to allow for demos with large amounts of data.

The following schemas are no longer updated, but are still available:

- OC: Online Catalog (archived) - a collection of object-relational database
                                  objects built inside the OE schema.
- OE: Order Entry (archived) - useful for dealing with matters of intermediate
                               complexity. Many data types are available in
                               this schema, including nonscalar data types.
- PM: Product Media (archived) - dedicated to print media data types.

1.1.1 Archived schemas

Archived schemas are provided for reference for examples in the documentation
but are no longer actively maintained.

1.2 Design Principles for Sample Schemas

The sample database schemas were created with the following design principles
in mind:

- Simplicity and ease of use. The HR schema is intentionally simple. It
  provides a graduated path from simple to intermediate levels of database use.

- Relevance for typical users. The schemas bring to the foreground the
  functionality that customers typically use. Only the most commonly used
  database objects are built automatically in the schemas. The entire set of
  schemas provides a foundation upon which you can expand to illustrate
  additional functionality.

- Extensibility. The sample schemas provide a logical and physical foundation
  for adding objects to demonstrate functionality beyond the fundamental scope.

- Relevance. The sample schemas are designed to be applicable to e-business
  and other significant industry trends.

1.3 User Benefits of Sample Schemas

Benefits provided by the sample schemas include the following:

- Continuity of context. When encountering the same set of tables everywhere,
  users, students, and developers can spend less time becoming familiar with
  the schema and more time understanding or explaining the technical concepts.

- Usability. Customers can use these schemas to run examples that are shown
  in Oracle Database documentation and training materials. This first-hand
  access to examples facilitates both conceptual understanding and
  application development.

- Quality. Through central maintenance and testing of the sample schemas and
  the examples that run against the schemas, the quality of Oracle Database
  documentation and training materials is enhanced.

========================================================================

2. Installing the Sample Schemas

    1. Go to the latest (or appropriate) release of the sample schemas
       (https://github.com/oracle-samples/db-sample-schemas/releases)
       and download the "Source code (zip)" file.
    2. Extract the downloaded .zip file.
    3. Inside the sample schemas main directory, navigate to the
       sample schema directory that you want to install and run
       the "<schema_name>_install.sql" script.
       (For more information about installing a schema, see the
	   "README.md" file in the schema directory.)
    4. Review the installation verification at the end of the installation
       or in the generated installation log file.

Example

To install the "HR" schema, complete the following steps:

1. "cd" into the "human_resources" folder.
2. Connect to the target database with a privileged user via SQLcl or SQL*Plus.
3. Run the "hr_install.sql" script and provide inputs for the prompts.

cd human_resources
sql <system>@<connect_string>
@hr_install.sql

========================================================================

3. Uninstalling the Sample Schemas

    1. Navigate to the sample schema folder that you want to uninstall
       and run the "<schema_name>_uninstall.sql" script.
       (For more information about uninstalling a schema, see the
	   "README.md" file in the schema directory.)
    2. Check the output for any errors during the uninstallation process.
