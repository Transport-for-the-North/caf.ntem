query
=====

.. argparse::
    :module: caf.ntem.__main__
    :func: _create_arg_parser
    :path: query


.. seealso::

    :ref:`build` for details on building the NTEM database from NTEM data files.


Config
------

The configuration file is a text file in YAML format that specifies the parameters
for running one, or multiple, queries against an NTEM database.

The configuration file is loaded and validated with :class:`caf.ntem.inputs.QueryArgs`,
and example of the configuration file is shown below.

.. literalinclude:: query-example.yml
    :language: yaml
    :caption: Example of the config file for the query command.
