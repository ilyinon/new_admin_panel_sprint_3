import logging


logging.Formatter = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)