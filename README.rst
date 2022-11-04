Cheshire: a Python Template Repository for RMI created by Catalyst Cooperative
=======================================================================================

.. readme-intro

.. image:: https://github.com/rmi-electricity/cheshire/workflows/tox-pytest/badge.svg
   :target: https://github.com/rmi-electricity/cheshire/actions?query=workflow%3Atox-pytest
   :alt: Tox-PyTest Status

.. image:: https://github.com/rmi-electricity/cheshire/workflows/docs/badge.svg
   :target: https://rmi-electricity.github.io/cheshire/
   :alt: GitHub Pages Status

.. image:: https://coveralls.io/repos/github/rmi-electricity/cheshire/badge.svg
   :target: https://coveralls.io/github/rmi-electricity/cheshire

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black>
   :alt: Any color you want, so long as it's black.

This template repository helps make new Python projects easier to set up and more
uniform. It contains a lot of infrastructure surrounding a minimal Python package named
``cheshire`` (the cat who isn't entirely there...). This template is mostly a lightly
modified copy of Catalyst Cooperative's
`cheshire <https://github.com/catalyst-cooperative/cheshire>`_ but with alterations
for private work and alternative tools.

Create a new repository from this template
=======================================================================================

* Choose a name for the new package that you are creating.
* The name of the repository should be the same as the name of the new Python package
  you are going to create. E.g. a repository at ``rmi-electricity/cheshire`` should
  be used to define a package named ``cheshire``.
* Fork this template repository to create a new Python project repo.
  `See these instructions <https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template>`__.
* Clone the new repository to your development machine.
* Run ``pre-commit install`` in the newly clone repository to install the
  `pre-commit hooks <https://pre-commit.com/>`__ defined in ``.pre-commit-config.yaml``
* Create the ``cheshire`` conda environment by running ``conda env create`` or
  (preferably) ``mamba env create`` in the top level of the repository.
* Activate the new conda environment with ``conda activate cheshire``.
* Run ``tox`` from the top level of the repository to verify that everything is working
  correctly.

Rename the package and distribution
=======================================================================================

Once you know that your forked version of the ``cheshire`` package is working as
expected, you should update the package and distribution names in your new repo to
reflect the name of your new package. The **package name** is determined by the name of
the directory under ``src/`` which contains the source code, and is the name you'll use
to import the package for use in a program, script, or notebook. E.g.:

.. code:: python

  import cheshire


The **distribution name** is the name that is used to install the software using a
program like  ``pip``, ``conda``, or ``mamba``. Because we do not generally distribute
our work in this way, this issue is not so important, but we use this naming convention
for internal consistency. It is often identical to the package
name, but can also contain a prefix namespace that indicates the individual or
organization responsible for maintaining the package. See :pep:`423` for more on
Python package naming conventions.  We are using the ``rmi`` namespace for the
packages that we publish, so our ``dispatch`` package would become ``rmi.dispatch``
in the Python Package Index (PyPI) or on ``conda-forge``. Because we do not generally
distribute our work publicly, this issue is not so important, but we use this naming
convention for internal consistency. The distribution name is determined
by the ``name`` argument under ``[metadata]`` in ``setup.cfg``.

The package and distribution names are referenced in many of the files in the template
repository, and they all need to be replaced with the name of your new package. You can
use ``grep -r`` to search recursively through all of the files for the word ``cheshire``
at the command line, or use the search-and-replace functionality of your IDE / text
editor. The name of the package directory under ``src/`` will also need to be changed.

* Rename the ``src/cheshire`` directory to reflect the new package name.
* Search for ``cheshire`` and replace it as appropriate everywhere. Sometimes
  this will be with a distribution name like ``rmi.cheshire``
  (the package as it would appear for ``pip`` or ``PyPI``) and sometimes this will be
  the importable package name (the name of the directory under ``src`` e.g.
  ``cheshire``)

What this template provides
=======================================================================================

Python Package Skeleton
-----------------------
* The ``src`` directory contains the code that will be packaged and deployed on the user
  system. That code is in a directory with the same name as the package.
* Using a separate ``src`` directory helps avoid accidentally importing the package when
  you're working in the top level directory of the repository.
* A simple python module (``dummy.py``), and a separate module providing a command line
  interface to that module (``cli.py``) are included as examples.
* Any files in the ``src/package_data/`` directory will also be packaged and deployed.
* What files are included in or excluded from the package on the user's system is
  controlled by the ``MANIFEST.in`` file and some options in ``setup.cfg``.
* The CLI is deployed using a ``console_script`` entrypoint defined in ``setup.cfg``.
* We use ``setuptools_scm`` to obtain the package's version directly from ``git`` tags,
  rather than storing it in the repository and manually updating it.
* ``README.rst`` is read in and used for the package's ``long_description``. This is
  what is would be displayed on the PyPI page for the package. For example, see the
  `PUDL Catalog <https://pypi.org/project/catalystcoop.pudl-catalog/0.1.0/>`__ page.
* By default we create at least three sets of "extras" -- additional optional package
  dependencies that can be installed in special circumstances: ``dev``, ``doc```, and
  ``tests``. The packages listed there are used in development, building the docs, and
  running the tests (respectively) but aren't required for a normal user who is just
  installing the package to use rather than develop.
* Python has recently evolved a more diverse community of build and packaging tools.
  Which flavor is being used by a given package is indicated by the contents of
  ``pyproject.toml``. That file also contains configuration for a few other tools,
  including ``bandit``, ``black``, ``isort``, and ``mypy``, described in the section
  on linters and formatters below.

Pytest Testing Framework
------------------------
* A skeleton `pytest <https://docs.pytest.org/>`_ testing setup is included in the
  ``tests/`` directory.
* Tests are split into ``unit`` and ``integration`` categories.
* Session-wide test fixtures, additional command line options, and other pytest
  configuration can be added to ``tests/conftest.py``
* Exactly what pytest commands are run during continuous integration is controlled by
  Tox.
* Pytest can also be run manually without using Tox, but will use whatever your
  personal python environment happens to be, rather than the one specified by the
  package. Running pytest on its own is a good way to debug new or failing tests
  quickly, but we should always use Tox and its virtual environment for actual testing.

Test Coordination with Tox
--------------------------
* We define several different test environments for use with Tox in ``tox.ini``
* `Tox <https://tox.wiki/en/latest/>`__ is used to run pytest in an isolated Python
  virtual environment.
* We also use Tox to coordinate running the code linters and building the documentation.
* The default Tox environment is named ``ci`` and it will run the linters, build the
  documentation, run all the tests, and generate test coverage statistics.
* ``tox.ini`` also contains sections near the bottom which configure the behavior of
  ``doc8``, ``flake8``, ``pytest``, and ``rstcheck``.

Git Pre-commit Hooks
--------------------
* A variety of sanity checks are defined as git pre-commit hooks -- they run any time
  you try to make a commit, to catch common issues before they are saved. Many of these
  hooks are taken from the excellent `pre-commit project <https://pre-commit.com/>`__.
* The hooks are configured in ``.pre-commit-config.yaml``
* For them to run automatically when you try to make a commit, you **must** install the
  pre-commit hooks in your cloned repository first. This only has to be done once.
* These checks are run as part of our CI, and the CI will fail if the pre-commit hooks
  fail.
* We also use the `pre-commit.ci <https://pre-commit.ci>`__ service to run the same
  checks on any code that is pushed to GitHub, and to apply standard code formatting
  to the PR in case it hasn't been run locally prior to being committed.

Additional comments on using Pre-commit
----------------------------------------------------
Most git GUI tools work with pre-commit but don't work that well. The terminal based
``git`` is usually the safer choice.

For this to work you must have a terminal session inside your repository folder. To
see what will be committed run ``git status``. To stage all files shown in red so
they will be included in the commit, run ``git add .``.

To make the commit run ``git commit -m '<commmit message>'``. If pre-commit hooks
alter the files, you will need to add those fixed files again (you can see this when
you run ``git status``) and then do the commit again.

Code Formatting
---------------
To avoid the tedium of meticulously formatting all the code ourselves, and to ensure as
standard style of formatting and syntactical idioms across the codebase, we use several
automatic code formatters, which run as pre-commit hooks. Many of them can also be
integrated directly into your text editor or IDE with the appropriate plugins. The
following formatters are included in the template ``.pre-commit-config.yaml``:

* `Use only absolute import paths <https://github.com/MarcoGorelli/absolufy-imports>`__
* `Standardize the sorting of imports <https://github.com/PyCQA/isort>`__
* `Remove unnecessary f-strings <https://github.com/dannysepler/rm_unneeded_f_str>`__
* `Upgrade type hints for built-in types <https://github.com/sondrelg/pep585-upgrade>`__
* `Upgrade Python syntax <https://github.com/asottile/pyupgrade>`__
* `Deterministic formatting with Black <https://github.com/psf/black>`__
* We also have a custom hook that clears Jupyter notebook outputs prior to committing.

Code & Documentation Linters
----------------------------
To catch errors before commits are made, and to ensure uniform formatting across the
codebase, we also use a bunch of different linters. They don't change the code or
documentation files, but they will raise an error or warning when something doesn't
look right so you can fix it.

* `bandit <https://bandit.readthedocs.io/en/latest/>`__ identifies code patterns known
  to cause security issues.
* `doc8 <https://github.com/pycqa/doc8>`__ and `rstcheck
  <https://github.com/myint/rstcheck>`__ look for formatting issues in our docstrings
  and the standalone ReStructuredText (RST) files under the ``docs/`` directory.
* `flake8 <https://github.com/PyCQA/flake8>`__ is an extensible Python linting
  framework, with a bunch of plugins.
* `mypy <https://mypy.readthedocs.io/en/stable/index.html>`__ Does static type checking,
  and ensures that our code uses type annotations.
* `pre-commit <https://pre-commit.com>`__ has a collection of built-in checks that `use
  pygrep to search Python files <https://github.com/pre-commit/pygrep-hooks>`__ for
  common problems like blanket ``# noqa`` annotations, as well as `language agnostic
  problems <https://github.com/pre-commit/pre-commit-hooks>`__ like accidentally
  checking large binary files into the repository or having unresolved merge conflicts.

Making ``bandit``, ``doc8``, ``flake8``, ``mypy``,  and ``rstcheck`` happy is work but
not always useful work. Sometimes you can edit their configurations to be less strict,
other times it makes sense to disable them. ``mypy`` can be a particular problem,
especially when you use ``pandas`` or ``numpy``.

Test Coverage
-------------
* We use Tox and the pytest `coverage <https://coverage.readthedocs.io>`__
  plugin to measure and record what percentage of our codebase is being tested, and to
  identify which modules, functions, and individual lines of code are not being
  exercised by the tests.
* When you run ``tox`` or ``tox -e ci`` (which is equivalent) a summary of the test
  coverage will be printed at the end of the tests (assuming they succeed). The full
  details of the test coverage is written to ``coverage.info``.
* When the tests are run via the ``tox-pytest`` workflow in GitHub Actions, the test
  coverage data from the ``coverage.info`` output is uploaded to a service called
  `Coveralls <https://coveralls.io>`__ that saves historical data about our test
  coverage, and provides a nice visual representation of the data -- identifying which
  subpackages, modules, and individual lines of are being tested. For example, here are
  the results
  `for the cheshire repo <https://coveralls.io/github/rmi-electricity/cheshire>`__.

Documentation Builds
--------------------
* We build our documentation using `Sphinx <https://www.sphinx-doc.org/en/master/>`__.
* Standalone docs files are stored under the ``docs/`` directory, and the Sphinx
  configuration is there in ``conf.py`` as well.
* We use `Sphinx AutoAPI <https://sphinx-autoapi.readthedocs.io/en/latest/>`__ to
  convert the docstrings embedded in the python modules under ``src/`` into additional
  documentation automatically.
* The top level documentation index simply includes this ``README.rst``, the
  ``LICENSE.txt`` and ``CODE_OF_CONDUCT.md`` files are similarly referenced. The only
  standalone documentation file under ``docs/`` right now is the ``release_notes.rst``.
* Unless you're debugging something specific, the docs should always be built using
  ``tox -e docs`` as that will lint the source files using ``doc8`` and ``rstcheck``,
  and wipe previously generated documentation to build everything from scratch. The docs
  are also rebuilt as part of the normal Tox run (equivalent to ``tox -e ci``).
* If you add something to the documentation generation process that needs to be cleaned
  up after, it should be integrated with the Sphinx hooks. There are some examples of
  how to do this at the bottom of ``docs/conf.py`` in the "custom build operations"
  section. For example, this is how we automatically regenerate the data dictionaries
  based on the PUDL metadata whenever the docs are built, ensuring that the docs stay up
  to date.

Documentation Publishing
------------------------
* We use the `GitHub Pages <https://pages.github.com>`__ service to host our
  documentation.
* When you open a PR or push to ``dev`` or ``main``, the associated
  documentation is automatically built and stored in a ``gh-pages`` branch.
* To make the documentation available, go to the repositories settings. Select
  'Pages' under 'Code and automation', select 'Deploy from a branch' and then
  select the ``gh-pages`` branch and then ``/(root)``, and click save.
* The documentation should then be available at
  https://rmi-electricity.github.io/<repo-name>/.

Dependabot
----------
We use GitHub's `Dependabot <https://docs.github.com/en/code-security/dependabot/dependabot-version-updates>`__
to automatically update the allowable versions of packages we depend on. This applies
to both the Python dependencies specified in ``setup.py`` and to the versions of the
`GitHub Actions <https://docs.github.com/en/actions>`__ that we employ. The dependabot
behavior is configured in ``.github/dependabot.yml``

GitHub Actions
--------------
Under ``.github/workflows`` are YAML files that configure the `GitHub Actions
<https://docs.github.com/en/actions>`__ associated with the repository. We use GitHub
Actions to:

* Run continuous integration using `tox <https://tox.wiki>`__ on several different
  versions of Python.
* Build and publish docs to github-pages.
