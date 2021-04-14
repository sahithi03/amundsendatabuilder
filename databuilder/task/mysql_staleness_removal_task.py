# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from typing import (
    Any, Dict, Set,
)

from amundsen_rds.models.base import Base
from pyhocon import ConfigFactory, ConfigTree
from sqlalchemy import (
    create_engine, func, inspect,
)
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import sessionmaker

from databuilder import Scoped
from databuilder.publisher.mysql_csv_publisher import MySQLCSVPublisher
from databuilder.task.base_task import Task

MYSQL_ENDPOINT = "mysql_endpoint"

TARGET_TABLES = "target_tables"
DRY_RUN = "dry_run"
# Staleness max percentage. Safety net to prevent majority of data being deleted.
STALENESS_MAX_PCT = "staleness_max_pct"
# Staleness max percentage per table. Safety net to prevent majority of data being deleted.
STALENESS_PCT_MAX_DICT = "staleness_max_pct_dict"
# Using this milliseconds and published timestamp to determine staleness
MS_TO_EXPIRE = "milliseconds_to_expire"
MIN_MS_TO_EXPIRE = "minimum_milliseconds_to_expire"

DEFAULT_CONFIG = ConfigFactory.from_dict({STALENESS_MAX_PCT: 5,
                                          TARGET_TABLES: [],
                                          STALENESS_PCT_MAX_DICT: {},
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
        self.target_tables = set(conf.get_list(TARGET_TABLES))
        self.target_table_model_dict = self._get_target_table_model_dict(self.target_tables)
        self.dry_run = conf.get_bool(DRY_RUN)
        self.staleness_max_pct = conf.get_int(STALENESS_MAX_PCT)
        self.staleness_max_pct_dict = conf.get(STALENESS_PCT_MAX_DICT)

        if MySQLCSVPublisher.JOB_PUBLISH_TAG in conf and MS_TO_EXPIRE in conf:
            raise Exception(f'Cannot have both {MySQLCSVPublisher.JOB_PUBLISH_TAG} and {MS_TO_EXPIRE} in job config')

        self.ms_to_expire = None
        if MS_TO_EXPIRE in conf:
            self.ms_to_expire = conf.get_int(MS_TO_EXPIRE)
            if self.ms_to_expire < conf.get_int(MIN_MS_TO_EXPIRE):
                raise Exception(f'{MS_TO_EXPIRE} is too small')
            self.marker = self.ms_to_expire
        else:
            self.marker = conf.get_string(MySQLCSVPublisher.JOB_PUBLISH_TAG)

        _engine = create_engine(conf.get_string(MYSQL_ENDPOINT))
        _session = sessionmaker(bind=_engine)
        self._session = _session()

    def _get_target_table_model_dict(self,
                                     target_tables: Set[str]
                                     ) -> Dict[str, DeclarativeMeta]:
        """
        Returns a dictionary with a table name to the corresponding RDS model class mapping.
        :param target_tables:
        :return:
        """
        target_table_model_dict = {}
        for model in Base._decl_class_registry.values():
            if hasattr(model, '__tablename__') and model.__tablename__ in target_tables:
                target_table_model_dict[model.__tablename__] = model
        return target_table_model_dict

    def run(self) -> None:
        """
        For each table specified in the config, performs a safety check to make sure this operation does not
        delete more than a threshold where default threshold is 5%. Once it passes a safety check, it deletes
        the stale records.
        :return:
        """
        sorted_table_names = [table.name for table in Base.metadata.sorted_tables]
        try:
            for table in reversed(sorted_table_names):
                if table in self.target_tables:
                    target_model_class = self.target_table_model_dict.get(table)
                    primary_key_column = self._get_primary_key(target_model_class=target_model_class)
                    staleness_pct = self._validate_record_staleness_pct(target_table=table,
                                                                        target_model_class=target_model_class,
                                                                        primary_key_column=primary_key_column)
                    if self.dry_run:
                        LOGGER.info('Skipping deleting records since it is a Dry Run.')
                        continue
                    if staleness_pct > 0:
                        self._delete_stale_records(target_table=table,
                                                   target_model_class=target_model_class)
        except Exception as e:
            self._session.rollback()
            raise e
        finally:
            self._session.close()

    def _validate_record_staleness_pct(self,
                                       target_table: str,
                                       target_model_class: DeclarativeMeta,
                                       primary_key_column: str
                                       ) -> int:
        """
        Validation method.Focused on limit the risk of deleting records.
        - Check if deleted records for a table will be within 5% of the total records in the table.
        :param target_table:
        :param target_model_class:
        :param primary_key_column:
        :return:
        """
        total_records_count = self._session.query(func.count(getattr(target_model_class, primary_key_column))).scalar()
        stale_records_count = self._session.query(func.count(getattr(target_model_class, primary_key_column))) \
            .filter(self._get_stale_records_filter_condition(target_model_class=target_model_class)).scalar()

        staleness_pct = 0
        if stale_records_count:
            staleness_pct = stale_records_count * 100 / total_records_count
            threshold = self.staleness_max_pct_dict.get(target_table, self.staleness_max_pct)
            if staleness_pct >= threshold:
                raise Exception(f'Staleness percentage of {target_table} is {staleness_pct} %. '
                                f'Stopping due to over threshold {threshold} %')
            LOGGER.info(f'Will be deleting {stale_records_count} {target_table} '
                        f'record(s) or {staleness_pct}% of {target_table} data.')
        else:
            LOGGER.info(f'No stale records in {target_table}')
        return staleness_pct

    def _delete_stale_records(self,
                              target_table: str,
                              target_model_class: DeclarativeMeta
                              ) -> None:
        try:
            deleted_records_count = self._session.query(target_model_class).filter(
                self._get_stale_records_filter_condition(target_model_class=target_model_class)).delete()
            self._session.commit()
            LOGGER.info(f'Deleted {deleted_records_count} record(s) of {target_table}')
        except Exception as e:
            LOGGER.exception(f'Failed to delete stale records for {target_table}')
            raise e

    def _get_stale_records_filter_condition(self,
                                            target_model_class: DeclarativeMeta
                                            ) -> Any:
        """
        Return the appropriate stale records filter condition depending on which field is used to expire stale data.
        :param target_model_class:
        :return:
        """
        if self.ms_to_expire:
            current_time = int(time.time() * 1000)
            filter_condition = getattr(target_model_class, 'publisher_last_updated_epoch_ms') < (
                current_time - self.marker)
        else:
            filter_condition = getattr(target_model_class, 'published_tag') != self.marker
        return filter_condition

    def _get_primary_key(self,
                         target_model_class: DeclarativeMeta
                         ) -> str:
        """
        Returns a primary key for the given RDS model class
        :param target_model_class:
        :return:
        """
        return [column.name for column in inspect(target_model_class).primary_key][0]
