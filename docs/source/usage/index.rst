Tool Usage
==========

.. attention::
    The tool usage docs pages are currently work-in-progress, if you have any suggestions
    for specific please consider raising an issue on
    `GitHub <https://github.com/transport-for-the-north/caf.ntem/issues>`__.

Graphical User Interface
------------------------

.. todo::
    Does CAF.ntem have a GUI?

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
