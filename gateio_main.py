# Filename: gateio_main.py

import subprocess

subprocess.run(["python", "gateio_folder_structure.py"])
subprocess.run(["python", "gateio_get_article_list.py"])
subprocess.run(["python", "gateio_get_articles.py"])
subprocess.run(["python", "gateio_get_json.py"])
subprocess.run(["python", "gateio_get_calendar.py"])
subprocess.run(["python", "gateio_archive_handler.py"])

