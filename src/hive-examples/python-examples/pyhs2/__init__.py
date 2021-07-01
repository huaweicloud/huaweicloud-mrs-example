def connect(*args, **kwargs):
    """
    Connect to the database; see connections.Connection.__init__() for
    more information.
    """
    from .connections import Connection
    return Connection(*args, **kwargs)