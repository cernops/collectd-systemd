import dbus
import collectd


class SystemD(object):
    def __init__(self):
        self.plugin_name = 'systemd'
        self.interval = 60.0
        self.verbose_logging = False
        self.services = []
        self.units = {}

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

    def get_unit(self, name):
        if name not in self.units:
            try:
                unit = dbus.Interface(self.bus.get_object('org.freedesktop.systemd1',
                                                          self.manager.GetUnit(name)),
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


    def configure_callback(self, conf):
        for node in conf.children:
            vals = [str(v) for v in node.values]
            if node.key == 'Service':
                self.services.extend(vals)
            elif node.key == 'Interval':
                self.interval = float(vals[0])
            elif node.key == 'Verbose':
                self.verbose_logging = (vals[0].lower() == 'true')
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

    def read_callback(self):
        self.log_verbose('Read callback called')
        for name in self.services:
            full_name = name + '.service'

            state = self.get_service_state(full_name)
            type = self.get_service_type(full_name)
            if state == 'broken':
                self.log_verbose ('Unit {0} reported as broken. Reinitializing the connection to dbus & retrying.'.format(full_name))
                self.init_dbus()
                state = self.get_service_state(full_name)

            value = (1.0 if state == 'running' or state == 'reload' or ( state == 'dead' and type == 'oneshot' ) else 0.0)
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
