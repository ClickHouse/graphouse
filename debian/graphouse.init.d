#!/bin/bash
#
# description: Starts and stops Yandex graphouse.

### BEGIN INIT INFO
# Provides:          graphouse
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start the graphouse.
# Description:       Start the graphouse.
### END INIT INFO


. /lib/lsb/init-functions
 
DAEMON="graphouse"
DAEMON_BASE="graphouse"
DAEMON_ROOT="/usr/bin"
 
case "$1" in
  start)
    log_begin_msg "Starting $DAEMON..."
    test -d /var/run/$DAEMON_BASE || mkdir -p /var/run/$DAEMON_BASE
    chown $DAEMON_BASE:nogroup /var/run/$DAEMON_BASE
 
    if start-stop-daemon --quiet --stop --signal 0 --pidfile /var/run/$DAEMON_BASE/$DAEMON.pid 2>/dev/null 1>/dev/null; then
      log_failure_msg "$DAEMON already running"
    else
      uf=/etc/yandex/$DAEMON/ulimit.conf
      if [ -x $uf ]; then
        log_action_msg "Loading ulimits from file" $uf
        . $uf
      fi
      su $DAEMON_BASE -c "/sbin/start-stop-daemon --start --exec $DAEMON_ROOT/$DAEMON.sh --make-pidfile --pidfile /var/run/$DAEMON_BASE/$DAEMON.pid --background"
      log_end_msg $?
    fi
  ;;
 
  stop)
    log_begin_msg "Stopping $DAEMON..."
    start-stop-daemon --quiet --retry 10 --stop --pidfile /var/run/$DAEMON_BASE/$DAEMON.pid 1>/dev/null 2>&1
    log_end_msg $?
    rm -f /var/run/$DAEMON_BASE/$DAEMON.pid 2>/dev/null
  ;;
 
  restart)
    log_begin_msg "Restarting $DAEMON..."
    $0 stop
    sleep 1
    $0 start
  ;;
 
*)
  log_success_msg "Usage $0 {start|stop|restart}"
  exit 1
 
esac
 
exit 0
