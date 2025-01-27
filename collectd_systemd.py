import dbus
import collectd
import re

class SystemD(object):
    def __init__(self):
        self.plugin_name = 'systemd'
        self.interval = 60.0
        self.verbose_logging = False
        self.scan_needreload = False
        self.needreload_ignore = []
        self.state_ignore_regex = []
        self.services = []
        self.units = {}
        self.manager_properties = None

    def log_verbose(self, msg):
        if not self.verbose_logging:
            return
        collectd.info('{} plugin [verbose]: {}'.format(self.plugin_name, msg))

    def init_dbus(self):
        self.units = {}
        self.bus = dbus.SystemBus()
        self.manager = dbus.Interface(self.bus.get_object('org.freedesktop.systemd1',
                                                          '/org/freedesktop/systemd1'),
                                      'org.freedesktop.systemd1.Manager')

    def get_unit(self, name, path=None):
        if name not in self.units:
            if not path:
                path = self.manager.LoadUnit(name)

            try:
                unit = dbus.Interface(self.bus.get_object('org.freedesktop.systemd1',
                                                          path),
                                      'org.freedesktop.DBus.Properties')
            except dbus.exceptions.DBusException as e:
                collectd.warning('{} plugin: failed to monitor unit {}: {}'.format(
                    self.plugin_name, name, e))
                return
            self.units[name] = unit
        return self.units[name]

    def get_service_state(self, name):
        unit = self.get_unit(name)
        if not unit:
            return 'broken'
        else:
            try:
                return unit.Get('org.freedesktop.systemd1.Unit', 'SubState')
            except dbus.exceptions.DBusException as e:
                self.log_verbose('{} plugin: failed to monitor unit {}: {}'.format(self.plugin_name, name, e))
                return 'broken'

    def get_service_type(self, name):
        unit = self.get_unit(name)
        if not unit:
            return 'broken'
        else:
            try:
                return unit.Get('org.freedesktop.systemd1.Service', 'Type')
            except dbus.exceptions.DBusException as e:
                self.log_verbose('{} plugin: failed to find type unit {}: {}'.format(self.plugin_name, name, e))
                return 'broken'

    def get_system_state(self):
        """
        Returns of system state of "broken" if more than one unit is in failed
        state otherwise the systemd is considered "running"
        """
        system_state = 'running'

        try:
            units = self.manager.ListUnits()
        except dbus.exceptions.DBusException as e:
            collectd.warning('{} plugin: failed to list units: {}'.format(
                self.plugin_name, e))
            return 'broken'

        for unit in units:
            name, _, _, _, _, _, path, _, _, _ = unit
            if any(re.search(pattern, name) for pattern in self.state_ignore_regex):
                continue
            unit = self.get_unit(name, path=path)
            try:
                state = unit.Get('org.freedesktop.systemd1.Unit', 'ActiveState')
            except dbus.exceptions.DBusException as e:
                collectd.warning('{} plugin: failed to get unit status {}: {}'.format(
                    self.plugin_name, name, e))
                systemd_state = 'broken'
            if state == 'failed':
                collectd.info('{} systemd_state plugin [info]: Unit status: {}, {}'.format(self.plugin_name, name,state))
                system_state = 'broken'

        return system_state

    def send_need_reload(self):
        try:
            units = self.manager.ListUnits()
        except dbus.exceptions.DBusException as e:
            collectd.warning('{} plugin: failed to list units: {}'.format(
                self.plugin_name, e))
            return False
        need_reload = False
        for unit in units:
            name, _, _, _, _, _, path, _, _, _ = unit
            if name in self.needreload_ignore:
                continue
            unit = self.get_unit(name, path=path)
            try:
                rel = unit.Get('org.freedesktop.systemd1.Unit', 'NeedDaemonReload')
            except dbus.exceptions.DBusException as e:
                collectd.warning('{} plugin: failed to get unit properties {}: {}'.format(
                    self.plugin_name, name, e))
                rel = True

            if rel:
                collectd.info('{} plugin [info]: Unit needs reload: {}'.format(self.plugin_name, name))
                need_reload = True

        # 1 = good 0 = bad
        need_reload = not need_reload

        val = collectd.Values(
                type='boolean',
                plugin=self.plugin_name,
                plugin_instance='needreload',
                type_instance='NeedDaemonReload',
                values=[need_reload],
        )
        val.dispatch()
        return True

    def configure_callback(self, conf):
        for node in conf.children:
            vals = [str(v) for v in node.values]
            if node.key == 'Service':
                self.services.extend(vals)
            elif node.key == 'Interval':
                self.interval = float(vals[0])
            elif node.key == 'Verbose':
                self.verbose_logging = (vals[0].lower() == 'true')
            elif node.key == 'ScanNeedReload':
                self.scan_needreload = (vals[0].lower() == 'true')
            elif node.key == 'NeedReloadIgnore':
                self.needreload_ignore.extend(vals)
            elif node.key == 'StateIgnoreRegex':
                self.state_ignore_regex.extend(vals)

            else:
                raise ValueError('{} plugin: Unknown config key: {}'
                                 .format(self.plugin_name, node.key))
        if not self.services:
            self.log_verbose('No services defined in configuration')
            return
        self.init_dbus()
        collectd.register_read(self.read_callback, self.interval)
        self.log_verbose('Configured with services={}, interval={}'
                         .format(self.services, self.interval))

    def send_system_state(self):
        state = self.get_system_state()
        if state == "broken":
            # Retry once more
            self.init_dbus()
            state = self.get_system_state()

        value = 0 if state == "broken" or state == "degraded" else 1
        self.log_verbose('Sending value: {}.systemd_state={} (state={}, type={})'
                         .format(self.plugin_name, value, state, type))

        stateval = collectd.Values(
                type='boolean',
                plugin=self.plugin_name,
                plugin_instance='systemd_state',
                type_instance='running',
                values=[value])
        stateval.dispatch()

    def read_callback(self):
        self.log_verbose('Read callback called')

        self.send_system_state()
        if self.scan_needreload:
            ok = self.send_need_reload()
            if not ok:
                # Dbus connection was broken, reconnect and retry
                self.init_dbus()
                self.send_need_reload()

        for name in self.services:
            full_name = name + '.service'

            state = self.get_service_state(full_name)
            type = self.get_service_type(full_name)
            if state == 'broken':
                self.log_verbose ('Unit {0} reported as broken. Reinitializing the connection to dbus & retrying.'.format(full_name))
                self.init_dbus()
                state = self.get_service_state(full_name)

            value = (1.0 if state == 'running' or state == 'auto-restart' or state == 'reload' or state == 'start' or ( state == 'dead' and type == 'oneshot' ) else 0.0)
            self.log_verbose('Sending value: {}.{}={} (state={}, type={})'
                             .format(self.plugin_name, name, value, state, type))
            val = collectd.Values(
                type='gauge',
                plugin=self.plugin_name,
                plugin_instance=name,
                type_instance='running',
                values=[value])
            val.dispatch()

mon = SystemD()
collectd.register_config(mon.configure_callback)
