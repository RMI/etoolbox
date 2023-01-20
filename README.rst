eToolBox: A set of tools and functions we use across projects
=======================================================================================

.. readme-intro

.. image:: https://github.com/rmi/etoolbox/workflows/tox-pytest/badge.svg
   :target: https://github.com/rmi/etoolbox/actions?query=workflow%3Atox-pytest
   :alt: Tox-PyTest Status

.. image:: https://github.com/rmi/etoolbox/workflows/docs/badge.svg
   :target: https://rmi.github.io/etoolbox/
   :alt: GitHub Pages Status

.. image:: https://coveralls.io/repos/github/RMI/etoolbox/badge.svg?branch=main
   :target: https://coveralls.io/github/RMI/etoolbox?branch=main

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black>
   :alt: Any color you want, so long as it's black.

.. image:: https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat
   :target: https://pycqa.github.io/isort/
   :alt: Imports: isort

Desciption
=======================================================================================

*  datazip

   *  DataZip - an extension of
      `ZipFile <https://docs.python.org/3/library/zipfile.html#zipfile-objects>`_ with
      a couple useful features:

      *  Support for easily storing and retrieving a range of Python objects, including
         builtins, pandas and numpy objects, and certain custom objects.

   *  IOMixin - a mixin that allows a class to be stored in a DataZip. Usually.

*  utils

   * arrays - pandas and numpy comparison aids.
   * match - helpers for Python's ``match`` syntax.

Installation
=======================================================================================

Dispatch can be installed and used in it's own environment or installed into another
environment using pip. To install it using pip:

.. code-block:: bash

   pip install git+https://github.com/rmi/etoolbox.git

Or from the dev branch:

.. code-block:: bash

   pip install git+https://github.com/rmi/etoolbox.git@dev


To create an environment for eToolbox, navigate to the repo folder in terminal and run:

.. code-block:: bash

   mamba update mamba
   mamba env create --name etoolbox --file environment.yml

If you get a ``CondaValueError`` that the prefix already exists, that means an
environment with the same name already exists. You must remove the old one before
creating the new one:

.. code-block:: bash

   mamba update mamba
   mamba env remove --name etoolbox
   mamba env create --name etoolbox --file environment.yml
