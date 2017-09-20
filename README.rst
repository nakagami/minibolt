===========
minibolt
===========

A simple Neo4j database driver.

Requirement
------------

CPython 3.5+ https://www.python.org or MicroPython https://micropython.org

Install
------------

::

   # pip install minibolt


Sample Code
------------

::

   >>> import minibolt
   >>> conn = minibolt.connect('servername', 'username', 'password')
   >>> rs = conn.run('MATCH (tom:Person {name: "Tom Hanks"})-[:ACTED_IN]->(movie) return movie.title')
   >>> conn.fields
   ['movie.title']
   >>> for r in rs:
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

NetworkX integration

See https://gist.github.com/nakagami/0cc142db992b6121fd123fa596cf6e90

