================
collectd-systemd
================

.. image:: https://travis-ci.org/mbachry/collectd-systemd.svg?branch=master
    :alt: Build status
    :target: https://travis-ci.org/mbachry/collectd-systemd

.. image:: https://coveralls.io/repos/github/mbachry/collectd-systemd/badge.svg?branch=master
    :alt: Coverage
    :target: https://coveralls.io/github/mbachry/collectd-systemd?branch=master

A `collectd`_ plugin which checks if given `systemd`_ services are one of
* state "running"
* state "reloading"
* state "state"
* state "dead" with a service type is "oneshot"
in each of these cases it sends sends `graphite`_ metrics of ``1.0``.
Otherwise it will send a ``0.0`` value.

The plugin is particularly useful together with `grafana's alerting`_.

.. _collectd: https://collectd.org/
.. _systemd: https://www.freedesktop.org/wiki/Software/systemd/
.. _graphite: https://graphite.readthedocs.io/en/latest/overview.html
.. _grafana's alerting: https://github.com/grafana/grafana/issues/2209

Quick start
-----------

Make sure Python dbus bindings are installed in your system:

* Debian/Ubuntu: ``sudo apt-get install python-dbus``

* Fedora/CentOS: ``sudo yum install dbus-python``

Copy ``collectd_systemd.py`` to collectd Python plugin directory
(usually ``/usr/lib64/collectd/python/`` or
``/usr/lib/collectd/python/``). Add following snippet do
``/etc/collectd.conf``::

    LoadPlugin python

    <Plugin python>
        ModulePath "/usr/lib64/collectd/python"
        Import "collectd_systemd"

        <Module collectd_systemd>
            Service sshd nginx postgresql
            Service httpd
        </Module>
    </Plugin>

If your service has dash in the name, you need to wrap that name in double
quotes::

    <Module collectd_systemd>
        Service "celery-bots" "gunicorn-data"
    </Module>

Restart collectd daemon and open grafana web ui. Add a new graph with
following query::

    aliasSub(collectd.*.systemd-*.gauge-running, '.+systemd-(.+)\..+', '\1')

You should see all configured systemd services in the graph. Now it's
enough to add an alert for values lower than ``1.0`` to be paged when
services are down.

Configuration
-------------

Following configuration options are supported:

* ``Service``: one or more systemd services to monitor. Separate
  multiple services with spaces. Multiple services lines can
  be specified when they will be concatinated.

* ``ScanNeedReload``: monitor the needreload status of all units (off by default)

* ``Interval``: check interval. It's ok to keep the default (60 seconds)

* ``Verbose``: enable verbose logging (off by default)


Metrics and Debug
-----------------

systemd-sshd/gauge-running
##########################

Each configured service (e.g. sshd) will be reported. If the value is less than one check

    systemctl status sshd

systemd-systemd-state/boolean-running
#####################################

Each node will be report this value. If the value is less than one check

    systemctl status 

    systemctl --state=failed

systemd-needreload/boolean-NeedDaemonReload
###########################################

Each node will report this value. If less than one then identify stale unit by looking in collectd log file for an entry

    systemd plugin [info]: Unit needs reload: certmgr-renew.timer

or by running the command

    for U in $(systemctl --no-pager --no-legend | awk '{print $1}' ) ; do
      systemctl show -p NeedDaemonReload -- $U  | grep -q yes && echo $U
    done

Running tests
-------------

Install tox using pip or Linux package manager.

Type ``tox`` to run tests.

Selinux
-------
On Redhat systems some selinux policy may be needed. Create
a file collectd_systemd.te::

    policy_module(collectd_systemd,0.1);
    require {
        type collectd_t;
        type initrc_exec_t;
    }
    dbus_session_client(system,collectd_t)
    init_status(collectd_t)
    init_dbus_chat(collectd_t)
    systemd_status_all_unit_files(collectd_t)
    allow collectd_t initrc_exec_t:service { status };

Create a file collectd_systemd.pp and install it::

   make -f /usr/share/selinux/devel/Makefile collectd_systemd.pp
   semodule -i collectd_systemd.pp

