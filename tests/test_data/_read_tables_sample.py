"""To test pudl table renaming in etoolbox."""

from etoolbox.utils.pudl import pd_read_pudl


def read_tables_sample():
    """Read tables."""
    tables = (
        "generators_eia860",
        "plants_eia",
        "denorm_utilities_eia",
    )
    for table in tables:
        yield pd_read_pudl(table)
