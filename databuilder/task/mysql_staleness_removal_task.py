# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from typing import Any, Optional

from amundsen_rds.models.base import Base
from pyhocon import ConfigFactory, ConfigTree
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import sessionmaker

from databuilder import Scoped
from databuilder.publisher.neo4j_csv_publisher import JOB_PUBLISH_TAG
from databuilder.task.base_task import Task

# MySql Endpoint
MYSQL_USER = 'mysql_user'
MYSQL_PASSWORD = 'mysql_password'
MYSQL_HOST = 'mysql_host'
MYSQL_PORT = 'mysql_port'
MYSQL_DB = 'db'
MYSQL_ENDPOINT = "mysql_endpoint"

TARGET_ENTITIES = "target_entities"
DRY_RUN = "dry_run"
# Staleness max percentage. Safety net to prevent majority of data being deleted.
STALENESS_MAX_PCT = "staleness_max_pct"
# Staleness max percentage per RDS model type. Safety net to prevent majority of data being deleted.
STALENESS_MAX_PCT_DICT = "staleness_max_pct_dict"
# Using this milliseconds and published timestamp to determine staleness
MS_TO_EXPIRE = "milliseconds_to_expire"
MIN_MS_TO_EXPIRE = "minimum_milliseconds_to_expire"

DEFAULT_CONFIG = ConfigFactory.from_dict({TARGET_ENTITIES: [],
                                          STALENESS_MAX_PCT: 5,
                                          STALENESS_MAX_PCT_DICT: {},
                                          MIN_MS_TO_EXPIRE: 86400000,
                                          DRY_RUN: False})

LOGGER = logging.getLogger(__name__)

MARKER_VAR_NAME = 'marker'


class MySQLStalenessRemovalTask(Task):
    """
    A Specific task to remove stale records in MySQL.
    It will use the "published_tag" attribute assigned from the MySqlCsvPublisher and if published tag is different from
    the one it is getting from the config, it will regard the record as stale.

    """

    def __init__(self) -> None:
        pass

    def get_scope(self) -> str:
        return 'task.remove_stale_data'

    def init(self, conf: ConfigTree) -> None:
        conf = Scoped.get_scoped_conf(conf, self.get_scope()) \
            .with_fallback(conf) \
            .with_fallback(DEFAULT_CONFIG)
        self.target_entities = set(conf.get_list(TARGET_ENTITIES))
        self.target_model_dict = {}
        for rds_model_class in Base.__subclasses__():
            rds_model_name = rds_model_class.__name__
            if rds_model_name in self.target_entities:
                self.target_model_dict[rds_model_name] = rds_model_class
        self.dry_run = conf.get_bool(DRY_RUN)
        self.staleness_max_pct = conf.get_int(STALENESS_MAX_PCT)
        self.staleness_max_pct_dict = conf.get(STALENESS_MAX_PCT_DICT)

        if JOB_PUBLISH_TAG in conf and MS_TO_EXPIRE in conf:
            raise Exception(f'Cannot have both {JOB_PUBLISH_TAG} and {MS_TO_EXPIRE} in job config')

        self.ms_to_expire = None
        if MS_TO_EXPIRE in conf:
            self.ms_to_expire = conf.get_int(MS_TO_EXPIRE)
            if self.ms_to_expire < conf.get_int(MIN_MS_TO_EXPIRE):
                raise Exception(f'{MS_TO_EXPIRE} is too small')
            self.marker = self.ms_to_expire
        else:
            self.marker = conf.get_string(JOB_PUBLISH_TAG)

        _engine = create_engine(conf.get_string(MYSQL_ENDPOINT))
        _session = sessionmaker(bind=_engine)
        self._session = _session()

    def run(self) -> None:
        """
        For each model specified in the config, performs a safety check to make sure this operation does not
        delete more than a threshold where default threshold is 5%. Once it passes a safety check, it deletes
        the stale records.
        :return:
        """
        for target_model_name, target_model_class in self.target_model_dict.items():
            self._validate_record_staleness_pct(target_model=target_model_name, target_model_class=target_model_class)
            self._delete_stale_records(target_model=target_model_name, target_model_class=target_model_class)

    def _validate_record_staleness_pct(self,
                                       target_model: str,
                                       target_model_class: DeclarativeMeta
                                       ) -> None:
        """
        Validation method.Focused on limit the risk of deleting records.
        - Check if deleted records for a table are within 5% of the total records in the table.
        :param target_model:
        :param target_model_class:
        :return:
        """

        total_records_count = self._session.query(target_model_class).count()
        stale_records_count = self._get_stale_records_statement(target_model_class=target_model_class).count()

        staleness_pct = stale_records_count * 100 / total_records_count
        threshold = self.staleness_max_pct_dict.get(target_model, self.staleness_max_pct)
        if staleness_pct >= threshold:
            raise Exception(f'Staleness percentage of {target_model} is {staleness_pct} %. '
                            f'Stopping due to over threshold {threshold} %')

    def _delete_stale_records(self,
                              target_model: str,
                              target_model_class: DeclarativeMeta
                              ) -> None:

        stale_records_count = self._get_stale_records_statement(target_model_class=target_model_class).count()
        if stale_records_count > 0:
            deleted_records_count = self._get_stale_records_statement(target_model_class=target_model_class).delete()
            LOGGER.info(f'Deleting {deleted_records_count} stale record(s) of {target_model}')
            if self.dry_run:
                LOGGER.info('Skipping deleting records since it is a Dry Run')
                return
        else:
            LOGGER.info(f'No stale records found for {target_model}')
        self._session.commit()
        self._session.close()

    def _get_stale_records_statement(self,
                                     target_model_class: DeclarativeMeta
                                     ) -> Optional[Any]:
        """
        Return the appropriate stale records statement depending on which field is used to expire stale data.
        :param target_model_class:
        :return:
        """
        if self.ms_to_expire:
            current_time = int(time.time() * 1000)
            stale_records_statement = self._session.query(target_model_class) \
                .filter(getattr(target_model_class, 'publisher_last_updated_epoch_ms') < (current_time - self.marker))
        else:
            stale_records_statement = self._session.query(target_model_class) \
                .filter(getattr(target_model_class, 'published_tag') != self.marker)
        return stale_records_statement
