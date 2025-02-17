import logging
import logging.handlers
import os
import tempfile


def prep_temp(tmp_dir):
    tmp_dir = os.path.join(tempfile.gettempdir(), tmp_dir)
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    tempfile.tempdir = tmp_dir
    logging.debug("tmpdir = %s", tmp_dir)


def setup_logging_and_temp(app_name, app_desc, base_name):
    log_file_name = base_name + ".log"

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
    stderr_handler.setLevel(logging.INFO)
    logging.root.addHandler(stderr_handler)

    rotating_handler = logging.handlers.TimedRotatingFileHandler(
        base_name + ".log", when="h", interval=6, backupCount=128
    )
    rotating_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
    rotating_handler.setFormatter(formatter)
    logging.root.addHandler(rotating_handler)

    logging.root.setLevel(logging.DEBUG)  # The lowest level of all the handlers.

    logging.info(app_name)
    logging.info(app_desc)
    logging.info("Logging to %s" % log_file_name)
    prep_temp(base_name)
