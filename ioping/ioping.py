
"""

IOPing plugin for Diamond.

Author: Antti Jaakkola

#### Dependencies
 * ioping


Create /usr/share/diamond/collectors/ioping directory and copy this plugin to it.

mkdir /usr/share/diamond/collectors/ioping
cp ioping/ioping.py /usr/share/diamond/collectors/ioping/

Create config file /etc/diamond/collectors/IOPing.conf with content:

enabled=True

Enjoy statistics!

"""

import diamond.collector
import subprocess

class IOPingCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(IOPingCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(IOPingCollector, self).get_default_config()
        config.update({
            'path':     'ioping',
            'directory': '/tmp'
        })
        self.arcstat = None
        return config

    def collect(self):
        """
        Overrides the Collector.collect method
        """
        path = self.config['directory']
        interval = int(self.config['interval']) / 3

        try:
            output = subprocess.check_output(['ioping', '-w', str(interval), '-q', path])
        except subprocess.CalledProcessError, err:
            self.log.info(
                'Could not get stats: %s' % err)
            self.log.exception('Could not get stats')
            return {}

        for line in output.splitlines():
            if line.startswith('min/avg/max/mdev'):
                # min/avg/max/mdev = 243 us / 438 us / 552 us / 92 us
                values = line.split("=")[1]
                values = dict(zip(['min', 'avg', 'max', 'mdev'],
                    [int(x.split()[0].strip()) for x in values.split("/")]))
                for key, value in values.items():
                    self.publish(key, value)



