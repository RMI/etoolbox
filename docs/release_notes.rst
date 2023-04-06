=======================================================================================
eToolBox Release Notes
=======================================================================================


.. _release-v0-2-0:

---------------------------------------------------------------------------------------
0.2.0 (2023-XX-XX)
---------------------------------------------------------------------------------------

What's New?
^^^^^^^^^^^
*  Complete redesign of system internals and standardization of the data format. This
   resulted in a couple key improvements:

   *  **Performance** Decoding is now lazy, so structures and objects are only
      rebuilt when they are retrieved, rather than when the file is opened. Encoding is
      only done once, rather than once to make sure it will work, and then
      again when the data is written on close. Further, the correct encoder/decoder is
      selected using :class:`dict` lookups rather than chains of :func:`isinstance`.
   *  **Data Format** Rather than a convoluted system to flatten the object
      hierarchy, we preserve the hierarchy in the ``__attributes__.json`` file. We also
      provide encoders and decoders that allows all Python builtins as well as other
      types to be stored in ``json``. Any data that cannot be encoded to ``json`` is
      saved elsewhere and the entry in ``__attributes__.json`` contains a pointer to
      where the data is actually stored. Further, rather than storing some metadata in
      ``__attributes__.json`` and some elsewhere, now **all** metadata is stored
      alongside the data or pointer in ``__attributes__.json``.
   *  **Custom Classes** We no longer save custom objects as their own
      :class:`.DataZip`. Their location in the object hierarchy is preserved with a
      pointer and associated metadata. The object's state is stored separately in a
      hidden key, ``__state__`` in ``__attributes__.json``.
   *  **References** The old format stored every object as many times as it
      was referenced. This meant that objects could be stored multiple times and when
      the hierarchy was recreated, these objects would be copies. The new process for
      storing custom classes, :class:`pandas.DataFrame`, :class:`pandas.Series`, and
      :class:`numpy.array` uses :func:`id` to make sure we only store data once and
      that these relationships are recreated when loading data from a :class:`.DataZip`.
   *  **API** :class:`.DataZip` behaves a little like a :class:`dict`. It
      has :meth:`.DataZip.get`, :meth:`.DataZip.items`, and :meth:`.DataZip.keys` which
      do what you would expect. It also implements dunder methods to allow membership
      checking using ``in``, :func:`len`, and subscripts to get and set items (i.e.
      ``obj[key] = value``) these all also behave as you would expect, except that
      setting an item raises a :class:`KeyError` if the key is already in use.
      One additional feature with lookups is that you can provide multiple keys which
      are looked up recursively allowing efficient access to data in nested structures.
      :meth:`.DataZip.dump` and :meth:`.DataZip.load` are static methods that allow you
      to directly save and load an object into a :class:`.DataZip`, similar to
      :func:`pickle.dump` and :func:`pickle.load` except they handle opening and
      closing the file as well. Finally, :meth:`.DataZip.replace` is a little like
      :meth:`typing.NamedTuple._replace`; it copies the contents of one
      :class:`.DataZip` into a new one, with select keys replaced.

*  Added dtype metadata for :mod:`pandas` objects as well as ability to ignore that
   metadata to allow use of ``pyarrow`` dtypes.
*  Switching to use :mod:`ujson` rather than the standard library version for
   performance.
*  Added optional support for :class:`polars.DataFrame` and :class:`polars.Series`.
*  Added :class:`.PretendPudlTabl` when passed as the ``klass`` argument to
   :meth:`.DataZip.load`, it allows accessing the dfs in a zipped :class:`pudl.PudlTabl`
   as you would normally but avoiding the :mod:`pudl` dependency.
*  Code cleanup along with adoption of `ruff <https://github.com/charliermarsh/ruff>`_
   and removal of bandit, flake8, isort, etc.
*  Added :func:`.lazy_import` to lazily import or proxy a module, inspired by
   :mod:`polars.dependencies.lazy_import`.
*  Created tools for proxying :class:`pudl.PudlTabl` to provide access to cached PUDL
   data without requiring that :mod:`pudl` is installed, or at least imported. The
   process of either loading a :class:`.PretendPudlTabl` from cache, or creating and
   then caching a :class:`pudl.PudlTabl` is handled by :func:`.make_pudl_tabl`.
*  Copied a number of helper functions that we often use  from :mod:`pudl.helpers` to
   :mod:`.pudl_helpers` so they can be used without installing or importing :mod:`pudl`.
*  Added a very light adaptation of the
   `python-remotezip <https://github.com/gtsystem/python-remotezip>`_ package to access
   files within a zip archive without downloading the full archive.
*  Updates to :class:`.DataZip` encoding and decoding of :class:`pandas.DataFrame` so
   they work with :mod:`pandas` version 2.0.0.

Bug Fixes
^^^^^^^^^
*  Allow :class:`typing.NamedTuple` to be used as keys in a :class:`dict`, and a
   :class:`collections.defaultdict`.
*  Fixed a bug in :func:`.make_pudl_tabl` where creating and caching a new
   :class:`pudl.PudlTabl` would fail to load the PUDL package.

Known Issues
^^^^^^^^^^^^
*  Some legacy :class:`.DataZip` files cannot be fully read, especially those with
   nested structures and custom classes.

.. _release-v0-1-0:

---------------------------------------------------------------------------------------
0.1.0 (2023-02-27)
---------------------------------------------------------------------------------------

What's New?
^^^^^^^^^^^
*  Migrating :class:`.DataZip` from
   `rmi.dispatch <https://github.com/rmi-electricity/dispatch>`_ where it didn't really
   belong. Also added additional functionality including recursive writing and reading
   of :class:`list`, :class:`dict`, and :class:`tuple` objects.
*  Created :class:`.IOMixin` and :class:`IOWrapper` to make it easier to add
   :class:`.DataZip` to other classes.
*  Migrating :func:`.compare_dfs` from the Hub.
*  Updates to :class:`.DataZip`, :class:`.IOMixin`, and :class:`IOWrapper` to better
   better manage attributes missing from original object or file representation of
   object. Including ability to use differently organized versions of
   :class:`.DataZip`.
*  Clean up of :class:`.DataZip` internals, both within the object and in laying out
   files. Particularly how metadata and attributes are stored. Added
   :meth:`.DataZip.readm` and :meth:`.DataZip.writem` to read and write additional
   metadata not core to :class:`.DataZip`.
*  Added support for storing :class:`numpy.array` objects in :class:`.DataZip` using
   :func:`numpy.load` and :func:`numpy.save`.
*  :class:`.DataZip` now handles writing attributes and metadata using
   :meth:`.DataZip.close` so :class:`.DataZip` can now be used with or without a
   context manager.
*  Added :func:`.isclose`, similar to :func:`numpy.isclose` but allowing comparison
   of arrays containing strings, especially useful with :class:`pandas.Series`.
*  Added a module :mod:`etoolbox.utils.match` containing the helpers Raymond Hettinger
   demonstrated in his `talk <https://www.youtube.com/watch?v=ZTvwxXL37XI>`_ at PyCon
   Italia for using Python's ``case``/``match`` syntax.
*  Added support for Python 3.11.
*  Added support for storing :mod:`plotly` figures as ``pdf`` in :class:`.DataZip`.
   :meth:`.DataZip.close` so :class:`.DataZip` can now be used with or without a
   context manager.
*  Added support for checking whether a file or attribute is stored in
   :class:`.DataZip` using :meth:`.DataZip.__contains__`, i.e. using Python's ``in``.
*  Added support for subscript-based, getting and setting data in :class:`.DataZip`.
*  Custom Python objects can be serialized with :class:`.DataZip` if they implement
   ``__getstate__`` and ``__setstate__``, or can be serialized using the default
   logic described in :meth:`object.__getstate__`. That default logic is now
   implemented in :meth:`.DataZip.default_getstate` and
   :meth:`.DataZip.default_setstate`. This replaces the use of ``to_file``
   and ``from_file`` by :class:`.DataZip`. :class:`.IOMixin` has been updated
   accordingly.
*  Added static methods :meth:`.DataZip.dump` and :meth:`.DataZip.load` for
   serializing a single Python object, these are designed to be similar to how
   :func:`pickle.dump` and :func:`pickle.load` work.
*  Removing :class:`.IOWrapper`.
*  Added a :meth:`.DataZip.replace` that copies the contents of an old
   :class:`.DataZip` into a new copy of it after which you can add to it.
*  Extended JSON encoding / decoding to process an expanded set of builtins,
   standard library, and other common objects including :class:`tuple`, :class:`set`,
   :class:`frozenset`, :class:`complex`, :class:`typing.NamedTuple`,
   :class:`datetime.datetime`, :class:`pathlib.Path`, and :class:`pandas.Timestamp`.
*  Adding centralized testing helpers.


Bug Fixes
^^^^^^^^^
*  Fixed an issue where a single column :class:`pandas.DataFrame` was recreated
   as a :class:`pandas.Series`. Now this should be backwards compatible by applying
   :class:`pandas.DataFrame.squeeze` if object metadata is not available.
*  Fixed a bug that prevented certain kinds of objects from working properly under
   3.11.
*  Fixed an issue where the name for a :class:`pandas.Series` might get mangled or
   changed.


Known Issues
^^^^^^^^^^^^
*  Recipe system is fragile and bespoke, there really should be a better way...
*  :class:`tuple` nested inside other objects may be returned as :class:`list`.
