
"""

ZFS arcstat plugin for Diamond.

Based on arcstat.py.

Author: Antti Jaakkola

#### Dependencies
 * ZFS


Create /usr/share/diamond/collectors/zfsarccollector directory and copy this plugin to it.

mkdir /usr/share/diamond/collectors/zfsarccollector
cp zfsarccollector/zfsarccollector.py /usr/share/diamond/collectors/zfsarccollector/

Create config file /etc/diamond/collectors/ZFSARCCollector.conf with content:

enabled=True

Enjoy statistics!

"""

import diamond.collector
import copy
from decimal import Decimal
from datetime import datetime, timedelta
import re

class arcstat(object):
    def __init__(self):
        self.kstat = {}
        self.cur = {}
        self.diff = {}
        self.prev = {}
        self.interval = 1
        self.last = None

        self.snap_stats()
        l2_size = self.cur.get("l2_size")
        if l2_size:
            self.l2exist = True
        else:
            self.l2exist = False

    def kstat_update(self):

        k = [line.strip() for line in open('/proc/spl/kstat/zfs/arcstats')]

        if not k:
            return

        del k[0:2]
        self.kstat = {}

        for s in k:
            if not s:
                continue

            name, unused, value = s.split()
            self.kstat[name] = Decimal(value)

    def snap_stats(self):

        now = datetime.now()
        if self.last:
            self.interval = (now - self.last).total_seconds()
        else:
            self.interval = 1
        self.interval = Decimal(self.interval)
        self.last = now
        prev = copy.deepcopy(self.cur)
        self.kstat_update()

        self.diff = {}
        self.cur = self.kstat
        for key in self.cur:
            if re.match(key, "class"):
                continue
            if key in self.prev:
                self.diff[key] = self.cur[key] - self.prev[key]
            else:
                self.diff[key] = self.cur[key]

    def calculate(self):

        self.snap_stats()
        v = dict()
        v["arc.hits"] = self.diff["hits"] / self.interval
        v["arc.miss"] = self.diff["misses"] / self.interval
        v["arc.read"] = v["arc.hits"] + v["arc.miss"]
        v["arc.hit_percent"] = 100 * v["arc.hits"] / v["arc.read"] if v["arc.read"] > 0 else 0
        v["arc.miss_percent"] = 100 - v["arc.hit_percent"] if v["arc.read"] > 0 else 0

        v["arc.dhit"] = (self.diff["demand_data_hits"] + self.diff["demand_metadata_hits"]) / self.interval
        v["arc.dmis"] = (self.diff["demand_data_misses"] + self.diff["demand_metadata_misses"]) / self.interval

        v["arc.dread"] = v["arc.dhit"] + v["arc.dmis"]
        v["arc.dh_percent"] = 100 * v["arc.dhit"] / v["arc.dread"] if v["arc.dread"] > 0 else 0
        v["arc.dm_percent"] = 100 - v["arc.dh_percent"] if v["arc.dread"] > 0 else 0

        v["arc.phit"] = (self.diff["prefetch_data_hits"] + self.diff["prefetch_metadata_hits"]) / self.interval
        v["arc.pmis"] = (self.diff["prefetch_data_misses"] +
                     self.diff["prefetch_metadata_misses"]) / self.interval

        v["arc.pread"] = v["arc.phit"] + v["arc.pmis"]
        v["arc.ph_percent"] = 100 * v["arc.phit"] / v["arc.pread"] if v["arc.pread"] > 0 else 0
        v["arc.pm_percent"] = 100 - v["arc.ph_percent"] if v["arc.pread"] > 0 else 0

        v["arc.mhit"] = (self.diff["prefetch_metadata_hits"] +
                     self.diff["demand_metadata_hits"]) / self.interval
        v["arc.mmis"] = (self.diff["prefetch_metadata_misses"] +
                     self.diff["demand_metadata_misses"]) / self.interval

        v["arc.mread"] = v["arc.mhit"] + v["arc.mmis"]
        v["arc.mh_percent"] = 100 * v["arc.mhit"] / v["arc.mread"] if v["arc.mread"] > 0 else 0
        v["arc.mm_percent"] = 100 - v["arc.mh_percent"] if v["arc.mread"] > 0 else 0

        v["arc.arcsz"] = self.cur["size"]
        v["arc.c"] = self.cur["c"]
        v["arc.mfu"] = self.diff["mfu_hits"] / self.interval
        v["arc.mru"] = self.diff["mru_hits"] / self.interval
        v["arc.mrug"] = self.diff["mru_ghost_hits"] / self.interval
        v["arc.mfug"] = self.diff["mfu_ghost_hits"] / self.interval
        v["arc.eskip"] = self.diff["evict_skip"] / self.interval
        v["arc.mtxmis"] = self.diff["mutex_miss"] / self.interval

        if self.l2exist:
            v["l2arc.hits"] = self.diff["l2_hits"] / self.interval
            v["l2arc.miss"] = self.diff["l2_misses"] / self.interval
            v["l2arc.read"] = v["l2arc.hits"] + v["l2arc.miss"]
            v["l2arc.hit_percent"] = 100 * v["l2arc.hits"] / v["l2arc.read"] if v["l2arc.read"] > 0 else 0

            v["l2arc.miss_percent"] = 100 - v["l2arc.hit_percent"] if v["l2arc.read"] > 0 else 0
            v["l2arc.asize"] = self.cur["l2_asize"]
            v["l2arc.size"] = self.cur["l2_size"]
            v["l2arc.bytes"] = self.diff["l2_read_bytes"] / self.interval

        return v


class ZFSARCCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(ZFSARCCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(ZFSARCCollector, self).get_default_config()
        config.update({
            'path':     'zfs'
        })
        self.arcstat = None
        return config

    def collect(self):
        """
        Overrides the Collector.collect method
        """

        if not self.arcstat:
            self.arcstat = arcstat()

        for key, value in self.arcstat.calculate().items():
            self.publish(key, value)

