from sqlalchemy import MetaData

_metadata = MetaData()


def metadata() -> MetaData:
    """Return the SQLAlchemy metadata populated as schema tables are implemented."""
    return _metadata
