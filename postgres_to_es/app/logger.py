import logging


# logging.Formatter = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(format = '%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)