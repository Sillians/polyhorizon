import great_expectations as gx

def get_ge_context():
    """
    Returns a filesystem-backed GE DataContext (GE 1.8 compatible).
    """
    return gx.get_context()
