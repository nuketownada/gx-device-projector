#!/usr/bin/env python3
"""Bench helper: record NameOwnerChanged for our well-known names to a JSONL file.

  noc_watcher.py <outfile>

Each line: {"name":..., "old":<owner>, "new":<owner>}. old=="" => name acquired;
new=="" => name lost. Filtered to the prefixes we care about.
"""

import sys
import json
import signal

import dbus
from gi.repository import GLib
from dbus.mainloop.glib import DBusGMainLoop

PREFIXES = ("com.victronenergy.", "com.hypnos.")


def main():
    outpath = sys.argv[1]
    f = open(outpath, "a", buffering=1)

    DBusGMainLoop(set_as_default=True)
    bus = dbus.SessionBus()

    def on_noc(name, old, new):
        if str(name).startswith(PREFIXES):
            f.write(json.dumps({"name": str(name), "old": str(old), "new": str(new)}) + "\n")

    bus.add_signal_receiver(
        on_noc, signal_name="NameOwnerChanged", dbus_interface="org.freedesktop.DBus"
    )

    loop = GLib.MainLoop()
    signal.signal(signal.SIGTERM, lambda *_: loop.quit())
    signal.signal(signal.SIGINT, lambda *_: loop.quit())
    print("noc_watcher ready", flush=True)
    loop.run()


if __name__ == "__main__":
    main()
