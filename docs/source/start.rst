Quick Start
===========

CAF.ntem is provided as a Python package and a command-line utility.
The command-line utility aims to make some of the commonly used functionality 
available without needing to use Python code, see :ref:`usage` for details.

CAF.ntem can be installed from pip, conda-forge or **pipx
(when using as a command-line utility).**

Pip
---
Installing through pip is easy and can be done in one command:
``pip install caf.ntem``

conda-forge
-----------
Installing through conda-forge is easy and can be done in one command:
``conda install caf.ntem -c conda-forge``

Pipx
----

Installing through pipx allows CAF.ntem to be used as a command-line utility
without needing to set up a full Python environment. This can be done in one command:
``pipx install caf.ntem``

.. tip::
    This is the recommended installation method when using CAF.ntem as a command-line utility,
    if you do not have pipx installed see `the pipx documentation <https://pipx.pypa.io/stable/>`_

Usage
-----

CAF.ntem functionality can be accessed via the CLI with the following command:

``caf.ntem <sub-command> [options]``

where ``<sub-command>`` is one of the available sub-commands (see :ref:`command-line interface`),
and ``[options]`` are the options for the chosen sub-command.

Python
^^^^^^

When using CAF.ntem functionality within Python:

.. code:: python

    import caf.ntem as ntem

The :ref:`user guide` contains :ref:`tutorials` and :ref:`code examples`, which
explain available functionality. For a detailed look at the
package API see :ref:`API Reference`.
