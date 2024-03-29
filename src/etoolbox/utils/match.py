"""Helpers for python's structural pattern matching.

These are the 'Miracle Tools' Raymond Hettinger describes in his PyCon [talk]_.
This code, description, and examples were adapted from the accompanying [docs]_.
As an introduction, Hettinger offers the following:

.. pull-quote::

   When thinking about structural pattern matching, take a “grammar first” approach.
   Everything in a case clause acts completely different from the same code outside
   of a case clause.

   • A variable name always triggers a capture pattern.
     It always assigns and always matches.
   • Parentheses always triggers a class pattern.
   • Dots always trigger a value pattern.
   • Square brackets always trigger a sequence pattern.
   • Curly braces always trigger a mapping pattern.
   • Literal numbers and strings always trigger an equality test.
   • ``None``, ``True``, and ``False`` always compare on identity.
   • ``_`` always triggers a wildcard pattern and matches everything.
     You can only have one of these and it must be last.

.. [talk] Hettinger, R. "Structural Pattern Matching in the Real World"
   PyCon Italia 2022. June 2022. Available:
   https://www.youtube.com/watch?v=ZTvwxXL37XI
.. [docs] Hettinger, R. PyItalia Pattern Matching Talk Documentation. June 2022.
   Available:
   https://www.dropbox.com/s/w1bs8ckekki9ype/PyITPatternMatchingTalk.pdf?dl=0
"""

import re


class Var:
    """:class:`.Var` provides a data holder for constants.

    >>> Var.x = 10
    >>> Var.x += 1
    >>> Var.x
    11
    >>> match 11:
    ...     case Var.x:
    ...         print('Matches "x"')
    ...     case _:
    ...         print("No match")
    Matches "x"
    """

    pass


class Const:
    """:class:`.Const` provides a data holder for constants.

    >>> Const.pi = 3.1415926535
    >>> Const.pi
    3.1415926535

    >>> Var.x = 11
    >>> match 3.1415926535:
    ...     case Const.pi:
    ...         print('Matches "pi"')
    ...     case Var.x:
    ...         print('Matches "x"')
    Matches "pi"
    """

    pass


class FuncCall:
    """Descriptor to convert ``fc.name`` to ``func(name)``.

    The :class:`FuncCall` class is a descriptor that passes the attribute name to
    function call. Here we pass the attribute names ``x`` and ``y`` to the :func:`ord`:

    >>> class A:
    ...     x = FuncCall(ord)
    ...     y = FuncCall(ord)
    >>> A.x
    120
    >>> A.y
    121

    This is used in case clauses to call arbitrary functions using the value pattern.

    This is needed when for impure functions where the value can change between
    successive calls (otherwise you could use :class:`.Const` or :class:`Var` tools
    shown above).

    For example, consider a language translation function that changes its result
    depending on the current language setting. We could create a namespace with dynamic
    lookups:

    .. code-block:: python

        class Directions:
            north = FuncCall(translate)
            south = FuncCall(translate)
            east = FuncCall(translate)
            west = FuncCall(translate)

    In the match/case statement, we use the value pattern to trigger a new function
    call:

    .. code-block:: python

        def convert(direction):
            match direction:
                case Directions.north:
                    return 1, 0
                case Directions.south:
                    return -1, 0
                case Directions.east:
                    return 0, 1
                case Directions.west:
                    return 0, -1
                case _:
                    raise ValueError(_("Unknown direction"))
            print("Adjustment:", adj)

    The tool is used like this:

    .. code-block:: python

        set_language("es")
        convert("sur")
        (-1, 0)

        set_language("fr")
        convert("nord")
        (1, 0)

    The case statements match the current language setting and will change when the
    language setting changes.
    """

    def __init__(self, func):
        """Init."""
        self.func = func

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, obj, objtype=None):
        return self.func(self.name)


class RegexEqual(str):
    """Override :meth:`str.__eq__` to match a regex pattern.

    The :class:`.RegexEqual` class inherits from :class:`str` and overrides the
    :meth:`str.__eq__` method to match a regular expression.

    >>> bool(RegexEqual("hello") == "h.*o")
    True

    This is used in the match-clause (not a case clause). It will match cases with a
    regex for a literal pattern:

    >>> match RegexEqual("the tale of two cities"):
    ...     case "s...y":
    ...         print("A sad story")
    ...     case "t..e":
    ...         print("A mixed tale")
    ...     case "s..a":
    ...         print("A long read")
    A mixed tale
    """

    def __eq__(self, pattern):
        return bool(re.search(pattern, self))


class InSet(set):
    """Override :meth:`set.__eq__` to test set membership.

    The :class:`.InSet` class inherits from set and overrides the :meth:`set.__eq__`
    method to test for set membership:

    >>> from types import SimpleNamespace
    >>> Colors = SimpleNamespace(
    ...     warm=InSet({"red", "orange", "yellow"}),
    ...     cool=InSet({"green", "blue", "indigo", "violet"}),
    ...     mixed=InSet({"purple", "brown"}),
    ... )
    >>> match "blue":
    ...     case Colors.warm:
    ...         print("warm")
    ...     case Colors.cool:
    ...         print("cool")
    ...     case Colors.mixed:
    ...         print("mixed")
    cool
    """

    def __eq__(self, elem):
        return elem in self
