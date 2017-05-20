===========
minibolt
===========

A simple Neo4j database driver.

Requirement
------------

CPython 3.5+ https://www.python.org or MicroPython https://micropython.org

Sample Code
------------

::

   >>> import minibolt
   >>> conn = minibolt.connect('localhost', 'neo4j', 'secret')
   >>> for r in conn.run('MATCH (tom:Person {name: "Tom Hanks"})-[:ACTED_IN]->(movie) return movie.title'):
   ...     print(r[0])
   ...
   Charlie Wilson's War
   The Polar Express
   A League of Their Own
   Cast Away
   Apollo 13
   The Green Mile
   The Da Vinci Code
   Cloud Atlas
   That Thing You Do
   Joe Versus the Volcano
   Sleepless in Seattle
   You've Got Mail
   >>>
