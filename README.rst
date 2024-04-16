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

.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json
    :target: https://github.com/astral-sh/ruff
    :alt: Ruff

Desciption
=======================================================================================

*  datazip

   *  `DataZip <https://rmi.github.io/etoolbox/autoapi/etoolbox/datazip/core/index.html#etoolbox.datazip.core.DataZip>`_
      - an extension of
      `ZipFile <https://docs.python.org/3/library/zipfile.html#zipfile-objects>`_ with
      a couple useful features:

      *  Support for easily storing and retrieving a range of Python objects, including
         builtins, pandas and numpy objects, and certain custom objects.
      *  Store dataframes as parquets for space efficiency.

   *  IOMixin - a mixin that allows a class to be stored in a DataZip. Usually.

*  utils

   * arrays - pandas and numpy comparison aids.
   * match - helpers for Python's ``match`` syntax.
   * pudl - tools for reading data from PUDL

Installation
=======================================================================================

Dispatch can be installed and used in it's own environment or installed into another
environment using pip. To install it using pip:

.. code-block:: bash

   pip install git+https://github.com/rmi/etoolbox.git

Or from the dev branch:

.. code-block:: bash

   pip install git+https://github.com/rmi/etoolbox.git@dev


.. warning::

   Version 0.2.0 is the last version that supports the legacy PUDL objects such as
   ``PudlTable`` and contains tools for replicating and working with those objects. You
   can install a specific version like this:

   .. code-block:: bash

      pip install git+https://github.com/rmi/etoolbox.git@0.2.0


To create an environment for eToolbox, navigate to the repo folder in terminal and run:

.. code-block:: bash

   mamba update mamba
   mamba env create --name etb --file environment.yml

If you get a ``CondaValueError`` that the prefix already exists, that means an
environment with the same name already exists. You must remove the old one before
creating the new one:

.. code-block:: bash

   mamba update mamba
   mamba env remove --name etb
   mamba env create --name etb --file environment.yml


PUDL Data Access
=======================================================================================
Setup
---------------------------------------------------------------------------------------
To use the new process for accessing PUDL data you will need to have the ``etoolbox``
library installed. This setup procedure only needs to be done once per user per machine.

Authentication to Google Cloud uses a service account access key which you will need to
get from Alex or Catalyst. Once you have that, run the following command in an
environment where ``etoolbox`` is installed.

.. code-block:: bash

   rmi-pudl-init <access_key>

Where ``<access_key>`` is the base64 encoding of of the service account access key as
a JSON obtained from Catalyst. This is stored in LastPass as ``PUDL Key (base64)`` in
the ``Shared-UTF`` folder.

Usage
---------------------------------------------------------------------------------------
Any table that is in the ``pudl.sqlite`` can be read using these functions without
needing to download the entire database.

.. code-block:: python

   from etoolbox.utils.pudl import pd_read_pudl

   df = pd_read_pudl("core_eia__codes_balancing_authorities")


More information about the tables are available in
`this data dictionary <https://catalystcoop-pudl.readthedocs.io/en/nightly/data_dictionaries/pudl_db.html#pudl-data-dictionary>`_.
New and old names for the tables are available
`here <https://docs.google.com/spreadsheets/d/1RBuKl_xKzRSLgRM7GIZbc5zUYieWFE20cXumWuv5njo/edit#gid=1126117325>`_.


GitHub Actions
---------------------------------------------------------------------------------------
To enable accessing PUDL data from tests run in GitHub Actions, additional steps are
required. Note: these instructions assume that you use ``pytest`` and ``tox``.

1. Make sure that the ``PUDL_ACCESS_KEY`` secret is available to your repository,
   this should be the case for all rmi-electricity repositories. Note: it will not
   automatically be available to forks of those repositories.
2. Add the following to the Action configuration file above where ``tox`` is run, you
   can see an example in ``.github/workflows/tox-pytest.yml``.

   .. code-block:: yaml

      env:
        PUDL_ACCESS_KEY: ${{ secrets.PUDL_ACCESS_KEY }}

3. Add the following to ``tox.ini`` in the global [testenv] section or at least the one
   where ``pytest`` runs, you can see an example in this repository.

   .. code-block::

      passenv = PUDL_ACCESS_KEY

4. Before any test that uses a PUDL access function runs, a special CI setup function
   must run. There are different ways to do this but this is one example that we use
   here.

   conftest.py

   .. code-block:: python

      from etoolbox.utils.pudl import rmi_pudl_init


      def pudl_access_key_setup(script_runner):  # noqa: PT004
          """Set up PUDL access key for testing."""
          rmi_pudl_init(os.environ.get("PUDL_ACCESS_KEY"))


   pudl_access_test.py

   .. code-block:: python

      from etoolbox.utils.pudl import pd_read_pudl


      @pytest.mark.usefixtures("pudl_access_key_setup")
      def test_pd_read_pudl_table():
         """Test reading table from GCS as :func:`pandas.DataFrame."""
         df = pd_read_pudl("core_eia__codes_balancing_authorities")
         assert not df.empty
