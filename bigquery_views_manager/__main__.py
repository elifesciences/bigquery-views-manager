import logging

from bigquery_views_manager.cli import main


if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    main()
