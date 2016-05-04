from collections import OrderedDict
import random
from unittest import TestCase
from unittest.mock import Mock, patch, sentinel

import dbus

from blivet.dbus.action import DBusAction
from blivet.dbus.blivet import DBusBlivet
from blivet.dbus.device import DBusDevice
from blivet.dbus.format import DBusFormat
from blivet.dbus.object import DBusObject
from blivet.dbus.constants import ACTION_INTERFACE, BLIVET_INTERFACE, DEVICE_INTERFACE, FORMAT_INTERFACE
from blivet.dbus.constants import ACTION_OBJECT_PATH_BASE
from blivet.dbus.constants import DEVICE_OBJECT_PATH_BASE, DEVICE_REMOVED_OBJECT_PATH_BASE
from blivet.dbus.constants import FORMAT_OBJECT_PATH_BASE, FORMAT_REMOVED_OBJECT_PATH_BASE


class DBusBlivetTestCase(TestCase):
    @patch.object(DBusObject, "_init_dbus_object")
    @patch("blivet.dbus.blivet.callbacks")
    def setUp(self, *args):  # pylint: disable=unused-argument
        self.dbus_object = DBusBlivet(Mock(name="ObjectManager"))
        self.dbus_object._blivet = Mock()

    def test_ListDevices(self):
        """ Verify that ListDevices returns what it should.

            It should return a dbus.Array w/ signature 'o' containing the
            dbus object path of each device in the DBusBlivet.
        """
        object_paths = dbus.Array([sentinel.dev1, sentinel.dev2, sentinel.dev3], signature='o')
        dbus_devices = OrderedDict((i, Mock(object_path=p, removed=False)) for (i, p) in enumerate(object_paths))
        self.dbus_object._dbus_devices = dbus_devices
        self.assertEqual(self.dbus_object.ListDevices(), object_paths)

        # now test the devices property for good measure. it should have the
        # same value.
        self.assertEqual(self.dbus_object.Get(BLIVET_INTERFACE, 'Devices'), object_paths)
        self.dbus_object._blivet.devices = Mock()

    def test_Reset(self):
        """ Verify that Reset calls the underlying Blivet's reset method. """
        self.dbus_object._blivet.reset_mock()
        self.dbus_object._blivet.devices = []
        self.dbus_object.Reset()
        self.dbus_object._blivet.reset.assert_called_once_with()
        self.dbus_object._blivet.reset_mock()

    def test_RemoveDevice(self):
        self.dbus_object._blivet.reset_mock()
        object_path = '/com/redhat/Blivet1/Devices/23'
        device_mock = Mock(name="device 23", object_path=object_path, removed=False)
        with patch.object(self.dbus_object, '_dbus_devices', new=dict()):
            self.dbus_object._dbus_devices[23] = device_mock
            self.dbus_object.RemoveDevice(object_path)

        self.dbus_object._blivet.devicetree.recursive_remove.assert_called_once_with(device_mock._device)
        self.dbus_object._blivet.reset_mock()

    def test_InitializeDisk(self):
        self.dbus_object._blivet.reset_mock()
        object_path = '/com/redhat/Blivet1/Devices/23'
        device_mock = Mock(name="device 23")
        device_mock.object_path = object_path
        device_mock.removed = False
        with patch.object(self.dbus_object, '_dbus_devices', new=dict()):
            self.dbus_object._dbus_devices[23] = device_mock
            self.dbus_object.InitializeDisk(object_path)

        self.dbus_object._blivet.devicetree.recursive_remove.assert_called_once_with(device_mock._device)
        self.dbus_object._blivet.initialize_disk.assert_called_once_with(device_mock._device)
        self.dbus_object._blivet.reset_mock()

    def test_Commit(self):
        self.dbus_object._blivet.reset_mock()
        self.dbus_object.Commit()
        self.dbus_object._blivet.do_it.assert_called_once_with()
        self.dbus_object._blivet.reset_mock()


@patch.object(DBusObject, 'connection')
class DBusObjectTestCase(TestCase):
    @patch.object(DBusObject, "_init_dbus_object")
    @patch("blivet.dbus.blivet.callbacks")
    def setUp(self, *args):  # pylint: disable=unused-argument
        self.obj = DBusObject(Mock(name="ObjectManager"))
        self.obj._manager.get_object_by_id.return_value = Mock(name="DBusObject", object_path="/an/object/path")

    def test_properties(self, *args):  # pylint: disable=unused-argument
        with self.assertRaises(NotImplementedError):
            _x = self.obj.properties

        with self.assertRaises(NotImplementedError):
            _x = self.obj.interface

        with self.assertRaises(NotImplementedError):
            _x = self.obj.object_path


@patch.object(DBusObject, 'connection')
@patch.object(DBusObject, 'add_to_connection')
@patch.object(DBusObject, 'remove_from_connection')
@patch("blivet.dbus.blivet.callbacks")
class DBusDeviceTestCase(DBusObjectTestCase):
    @patch.object(DBusObject, "_init_dbus_object")
    def setUp(self, *args):
        self._device_id = random.randint(0, 500)
        self._format_id = random.randint(501, 1000)
        self.obj = DBusDevice(Mock(name="StorageDevice", id=self._device_id,
                                   parents=[], children=[]),
                              Mock(name="OjectManager"))
        self.obj._manager.get_object_by_id.return_value = Mock(name="DBusObject", object_path="/an/object/path")

    @patch('dbus.UInt64')
    def test_properties(self, *args):  # pylint: disable=unused-argument
        self.assertTrue(isinstance(self.obj.properties, dict))
        self.assertEqual(self.obj.interface, DEVICE_INTERFACE)
        self.assertEqual(self.obj.object_path, "%s/%d" % (DEVICE_OBJECT_PATH_BASE, self._device_id))
        self.obj.set_presence(False)
        self.assertEqual(self.obj.object_path, "%s/%d" % (DEVICE_REMOVED_OBJECT_PATH_BASE, self._device_id))
        self.obj.set_presence(True)
        self.assertEqual(self.obj.object_path, "%s/%d" % (DEVICE_OBJECT_PATH_BASE, self._device_id))


@patch.object(DBusObject, 'connection')
@patch.object(DBusObject, 'add_to_connection')
@patch.object(DBusObject, 'remove_from_connection')
@patch("blivet.dbus.blivet.callbacks")
class DBusFormatTestCase(DBusObjectTestCase):
    @patch.object(DBusObject, "_init_dbus_object")
    def setUp(self, *args):
        self._format_id = random.randint(0, 500)
        self.obj = DBusFormat(Mock(name="DeviceFormat", id=self._format_id),
                              Mock(name="ObjectManager"))
        self.obj._manager.get_object_by_id.return_value = Mock(name="DBusObject", object_path="/an/object/path")

    def test_properties(self, *args):  # pylint: disable=unused-argument
        self.assertTrue(isinstance(self.obj.properties, dict))
        self.assertEqual(self.obj.interface, FORMAT_INTERFACE)
        self.assertEqual(self.obj.object_path, "%s/%d" % (FORMAT_OBJECT_PATH_BASE, self._format_id))
        self.obj.set_presence(False)
        self.assertEqual(self.obj.object_path, "%s/%d" % (FORMAT_REMOVED_OBJECT_PATH_BASE, self._format_id))
        self.obj.set_presence(True)
        self.assertEqual(self.obj.object_path, "%s/%d" % (FORMAT_OBJECT_PATH_BASE, self._format_id))


@patch("blivet.dbus.blivet.callbacks")
class DBusActionTestCase(DBusObjectTestCase):
    @patch.object(DBusObject, "_init_dbus_object")
    def setUp(self, *args):
        self._id = random.randint(0, 500)
        self.obj = DBusAction(Mock(name="DeviceAction", id=self._id), Mock(name="ObjectManager"))
        self.obj._manager.get_object_by_id.return_value = Mock(name="DBusObject", object_path="/an/object/path")

    def test_properties(self, *args):  # pylint: disable=unused-argument
        self.assertTrue(isinstance(self.obj.properties, dict))
        self.assertEqual(self.obj.interface, ACTION_INTERFACE)
        self.assertEqual(self.obj.object_path, "%s/%d" % (ACTION_OBJECT_PATH_BASE, self._id))
