import logging

def configure_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create and add a file handler
    fh = logging.FileHandler("logs/etl_errors.log", mode="a")
    fh.setLevel(logging.WARNING)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    ch.setFormatter(fmt)
    fh.setFormatter(fmt)

    root.handlers.clear()
    root.addHandler(ch)
    root.addHandler(fh)