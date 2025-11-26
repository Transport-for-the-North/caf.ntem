query
=====

.. todo:: Write usage instructions for the ``caf.ntem query`` command.

.. argparse::
    :module: caf.ntem.__main__
    :func: _create_arg_parser
    :path: query


Config
------

The configuration file is a text file in YAML format that specifies the parameters
for running one, or multiple, queries against an NTEM database.

Example of the configuration file:

.. include:: query-example.yml
    :literal:
