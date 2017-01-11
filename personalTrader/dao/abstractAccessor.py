# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod


class Accessor():
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_hq(self, code, start_date=None, end_date=None, retry_times=3):
        """
        get the historical daily data. It always retrieve the data from internal database first up to the checkpoint,
        then request the rest data from external data source.
        :param code: string number code without pre/suffix, e.g. '600033'
        :param start_date: string in format '%Y-%m-%d', e.g. '2017-01-05'. optional.
        :param end_date: string in format '%Y-%m-%d', e.g. '2017-01-05'. optional.
        :param retry_times: int. default = 3.
        :return: dataframe indexed by 'Date' (sorted), with columns ['Date', 'Open', 'High', 'Close', 'Low', 'Volume', 'Amount'].
                 The datetypes are [datetime, float, float, float, float, int, float]
                 It can return an empty DataFrame if there is any issue.
        """
        pass

    @abstractmethod
    def get_fhps(self, code, start_date=None, end_date=None, retry_times=3):
        """
        get the historcial fhps data, data are base on per share (instead of per 10 shares)
        It always retrieve the data from internal database first up to the checkpoint,
        then request the rest data from external data source.
        :param code: string number code without pre/suffix, e.g. '600033'
        :param start_date: optional
        :param end_date: optional
        :param retry_times: int. default = 3.
        :return: dataframe indexed by 'Date' (sorted), with columns ['Date', 'Fh', 'Ps'].
                 Datetypes are [datetime, float, float]
                 It can return an empty DataFrame if there is any issue.
        """
        pass


class InternalAccessor(Accessor):
    @abstractmethod
    def get_checkpoints(self, code, data_source):
        """
        Returns the check points for the accessor_name, in order to avoid repeated download
        :param code: string number code without pre/suffix, i.e. '600033'
        :param data_source: string for the source, e.g. 'sohu'
        :return: datetime If never checked, return datetime(1980, 1, 1) by default
        """
