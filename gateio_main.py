# Filename: gateio_main.py
import logging

import gateio_logger_setup
import gateio_folder_structure  
import gateio_get_article_list
import gateio_get_articles
import gateio_get_json
import gateio_get_calendar
import gateio_archive_handler

gateio_logger_setup.setup_logging()
logger = logging.getLogger()

logger.info(f"\n\n|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
logger.error(f"\n\n|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")

gateio_folder_structure
gateio_get_article_list.main()
gateio_get_articles.main()
gateio_get_json.main()  
gateio_get_calendar.main()
gateio_archive_handler.main()
