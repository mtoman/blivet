import dbus.service

from blivet import Blivet

BLIVET_OBJECT_PATH = "/org/blivet/Blivet"
BUS_NAME = "org.blivet.Blivet"
BLIVET_INTERFACE = "org.blivet.Blivet"

class BlivetService(dbus.service.Object):
    def __init__(self):
        super().__init__(bus_name=dbus.service.BusName(BUS_NAME, dbus.SystemBus()),
                         object_path=BLIVET_OBJECT_PATH)
        self._blivet = Blivet()

    @dbus.service.method(dbus_interface=BLIVET_INTERFACE)
    def reset(self):
        self._blivet.reset()

    @dbus.service.method(dbus_interface=BLIVET_INTERFACE, out_signature='as')
    def listDeviceNames(self):
        return [str(d) for d in self._blivet.devices]

    @dbus.service.method(dbus_interface=BLIVET_INTERFACE, in_signature='s', out_signature='s')
    def resolveDevice(self, spec):
        device = self._blivet.devicetree.resolve_device(spec)
        return str(device)
