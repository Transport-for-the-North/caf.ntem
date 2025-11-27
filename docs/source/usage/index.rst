Tool Usage
==========

CAF.ntem provides functionality to load NTEM datasets into a SQlite database and
query the database to produce planning, trip end and car ownership datasets. The
tools functionality is provided via a command-line interface (CLI) with multiple
sub-commands.

.. seealso::

   - :ref:`quick start` for instructions on installation.
   - :ref:`tutorials` for more detailed tutorials on using CAF.ntem.
   - :ref:`code examples` for example code snippets using CAF.ntem.
   - :ref:`api reference` for details on the package API.


Command-Line Interface
----------------------

CAF.ntem provides a command-line interface (CLI) for building a SQlite database from NTEM
data files and querying it to get planning, trip end and car ownership datasets.

.. argparse::
    :module: caf.ntem.__main__
    :func: _create_arg_parser
    :nosubcommands:


Sub-commands
^^^^^^^^^^^^

The following functionality is accessible vis sub-commands:

.. toctree::
    :maxdepth: 1

    build
    build-config
    query

.. note::
    Both the ``build`` and ``build-config`` commands run the same process they
    just allow the parameters to be specified in different ways.
