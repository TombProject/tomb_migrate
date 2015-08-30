tomb_migrate
============
Run db migrations!

Getting Started
===============
Every command supports the ``--database`` flag if you want to limit the
operation to a single database. The ``revision`` command requires a
``--database`` flag be passed in.

Make the database config
-------------------------
You need to add a ``databases`` section to your ``application`` section:

.. code-block:: yaml

    databases:
        primary:
            type: rethinkdb
            host: 127.0.0.1
            port: 1337
            db: test

        secondary:
            type: postgresql
            host: 127.0.0.1
            port: 1336
            db: test

Setup tracking tables
---------------------

.. code-block:: bash

    $ tomb db init

Create new revision
-------------------

.. code-block:: bash

    $ tomb db revision -m "create user table" -d <db name>

Upgrade database to latest revision
-----------------------------------

.. code-block:: bash

    $ tomb db upgrade [-d <db name>]

Downgrade to previous version
-----------------------------

.. code-block:: bash

    $ tomb db downgrade [-d <db name>]

Downgrade to a specific revision
--------------------------------

.. code-block:: bash

    $ tomb db downgrade -r <whatever>  [-d <db name>]

Drop the database
-----------------

.. code-block:: bash

    $ tomb db drop [-d <db name>]
