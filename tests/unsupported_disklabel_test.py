# vim:set fileencoding=utf-8

import unittest
from unittest.mock import patch, sentinel, DEFAULT

from blivet.actionlist import ActionList
from blivet.blivet import Blivet
from blivet.deviceaction import ActionDestroyFormat
from blivet.devices import DiskDevice
from blivet.devices import LVMLogicalVolumeDevice
from blivet.devices import LVMVolumeGroupDevice
from blivet.devices import PartitionDevice
from blivet.devicetree import DeviceTree
from blivet.flags import flags
from blivet.formats import get_format
from blivet.size import Size


class UnsupportedDiskLabelTestCase(unittest.TestCase):
    def setUp(self):
        disk1 = DiskDevice("testdisk", size=Size("300 GiB"), exists=True,
                           fmt=get_format("Unsupported Disk Label", exists=True))

        with self.assertLogs("blivet", level="INFO") as cm:
            partition1 = PartitionDevice("testpart1", size=Size("150 GiB"), exists=True,
                                         parents=[disk1], fmt=get_format("ext4", exists=True))
        self.assertTrue("disklabel is unsupported" in "\n".join(cm.output))

        with self.assertLogs("blivet", level="INFO") as cm:
            partition2 = PartitionDevice("testpart2", size=Size("100 GiB"), exists=True,
                                         parents=[disk1], fmt=get_format("lvmpv", exists=True))
        self.assertTrue("disklabel is unsupported" in "\n".join(cm.output))

        # To be supported, all of a devices ancestors must be supported.
        disk2 = DiskDevice("testdisk2", size=Size("300 GiB"), exists=True,
                           fmt=get_format("lvmpv", exists=True))

        vg = LVMVolumeGroupDevice("testvg", exists=True, parents=[partition2, disk2])

        lv = LVMLogicalVolumeDevice("testlv", exists=True, size=Size("64 GiB"),
                                    parents=[vg], fmt=get_format("ext4", exists=True))

        self.disk1 = disk1
        self.disk2 = disk2
        self.partition1 = partition1
        self.partition2 = partition2
        self.vg = vg
        self.lv = lv

    def test_unsupported_disklabel(self):
        """ Test behavior of partitions on unsupported disklabels. """
        # Verify some basic properties of the partitions.
        self.assertFalse(self.partition1.disklabel_supported)
        self.assertFalse(self.partition2.disklabel_supported)
        self.assertEqual(self.partition1.disk, self.disk1)
        self.assertEqual(self.partition2.disk, self.disk1)
        self.assertIsNone(self.partition1.parted_partition)
        self.assertIsNone(self.partition2.parted_partition)
        self.assertFalse(self.partition1.is_magic)
        self.assertFalse(self.partition2.is_magic)
        self.assertTrue(self.disk1.supported)
        self.assertTrue(self.disk2.supported)
        self.assertFalse(self.partition1.supported)
        self.assertFalse(self.partition2.supported)
        self.assertFalse(self.vg.supported)
        self.assertFalse(self.lv.supported)

        # Verify that probe returns without changing anything.
        partition1_type = sentinel.partition1_type
        self.partition1._part_type = partition1_type
        self.partition1.probe()
        self.assertEqual(self.partition1.part_type, partition1_type)
        self.partition1._part_type = None

        # partition1 is not resizable even though it contains a resizable filesystem
        self.assertEqual(self.partition1.resizable, False)

        # lv is resizable as usual
        with patch.object(self.lv.format, "_resizable", new=True):
            self.assertEqual(self.lv.resizable, True)

        # the lv's destroy method should call blockdev.lvm.lvremove as usual
        with patch.object(self.lv, "_pre_destroy"):
            with patch("blivet.devices.lvm.blockdev.lvm.lvremove") as lvremove:
                self.lv.destroy()
                self.assertTrue(lvremove.called)

        # the vg's destroy method should call blockdev.lvm.vgremove as usual
        with patch.object(self.vg, "_pre_destroy"):
            with patch.multiple("blivet.devices.lvm.blockdev.lvm",
                                vgreduce=DEFAULT,
                                vgdeactivate=DEFAULT,
                                vgremove=DEFAULT) as mocks:
                self.vg.destroy()
        self.assertTrue(mocks["vgreduce"].called)
        self.assertTrue(mocks["vgdeactivate"].called)
        self.assertTrue(mocks["vgremove"].called)

        # the partition's destroy method shouldn't try to call any disklabel methods
        with patch.object(self.partition2, "_pre_destroy"):
            with patch.object(self.partition2.disk, "original_format") as disklabel:
                self.partition2.destroy()
        self.assertEqual(len(disklabel.mock_calls), 0)
        self.assertTrue(self.partition2.exists)

        # Destroying the disklabel should set all partitions to non-existing.
        # XXX This part is handled by ActionList.
        actions = ActionList()
        unsupported_disklabel = self.disk1.format
        actions.add(ActionDestroyFormat(self.disk1))
        self.assertTrue(self.disk1.format.exists)
        self.assertTrue(self.partition1.exists)
        self.assertTrue(self.partition2.exists)
        with patch.object(unsupported_disklabel, "_pre_destroy"):
            with patch.object(unsupported_disklabel, "_destroy") as destroy:
                with patch.object(actions, "_pre_process"):
                    with patch.object(actions, "_post_process"):
                        actions.process(devices=[self.partition1, self.partition2, self.disk1])

        self.assertTrue(destroy.called)
        self.assertFalse(unsupported_disklabel.exists)
        self.assertFalse(self.partition1.exists)
        self.assertFalse(self.partition2.exists)

    def test_recursive_remove(self):
        devicetree = DeviceTree()
        devicetree._add_device(self.disk1)
        devicetree._add_device(self.partition1)
        devicetree._add_device(self.partition2)
        devicetree._add_device(self.disk2)
        devicetree._add_device(self.vg)
        devicetree._add_device(self.lv)

        self.assertIn(self.disk1, devicetree.devices)
        self.assertIn(self.partition1, devicetree.devices)
        self.assertIn(self.lv, devicetree.devices)
        self.assertEqual(devicetree.get_device_by_name(self.disk1.name), self.disk1)
        self.assertIsNotNone(devicetree.get_device_by_name(self.partition1.name))
        self.assertIsNotNone(devicetree.get_device_by_name(self.partition1.name, hidden=True))
        self.assertIsNotNone(devicetree.get_device_by_name(self.lv.name, hidden=True))
        self.assertIsNotNone(devicetree.get_device_by_path(self.lv.path, hidden=True))
        self.assertIsNotNone(devicetree.get_device_by_id(self.partition2.id, hidden=True,
                                                         incomplete=True))
        self.assertEqual(len(devicetree.get_dependent_devices(self.disk1)), 4)
        with patch('blivet.devicetree.ActionDestroyFormat.apply'):
            devicetree.recursive_remove(self.disk1)
            self.assertTrue(self.disk1 in devicetree.devices)
            self.assertFalse(self.partition1 in devicetree.devices)
            self.assertFalse(self.partition2 in devicetree.devices)
            self.assertFalse(self.vg in devicetree.devices)
            self.assertFalse(self.lv in devicetree.devices)

    def test_hide_unsupported_devices_flag(self):
        flags.hide_unsupported_devices = True

        devicetree = DeviceTree()
        devicetree._add_device(self.disk1)
        devicetree._add_device(self.partition1)
        devicetree._add_device(self.partition2)
        devicetree._add_device(self.disk2)
        devicetree._add_device(self.vg)
        devicetree._add_device(self.lv)

        # With flags.hide_unsupported_devices the disk should still be visible
        # but none of the other devices built on it should.
        self.assertIn(self.disk1, devicetree.devices)
        self.assertNotIn(self.partition1, devicetree.devices)
        self.assertNotIn(self.lv, devicetree.devices)
        self.assertEqual(devicetree.get_device_by_name(self.disk1.name), self.disk1)
        self.assertIsNone(devicetree.get_device_by_name(self.partition1.name))
        self.assertIsNone(devicetree.get_device_by_name(self.partition1.name, hidden=True))
        self.assertIsNone(devicetree.get_device_by_name(self.lv.name, hidden=True))
        self.assertIsNone(devicetree.get_device_by_path(self.lv.path, hidden=True))
        self.assertIsNone(devicetree.get_device_by_id(self.partition2.id, hidden=True, incomplete=True))
        self.assertEqual(len(devicetree.get_dependent_devices(self.disk1)), 0)
        self.assertTrue(self.vg in self.partition2.children)  # flag doesn't affect children or parents
        self.assertTrue(self.partition2 in self.disk1.children)
        # recursive_remove should find the descendants even though the above queries fail
        with patch('blivet.devicetree.ActionDestroyFormat.apply'):
            devicetree.recursive_remove(self.disk1)
            self.assertTrue(self.disk1 in devicetree._devices)
            self.assertFalse(self.partition1 in devicetree._devices)
            self.assertFalse(self.partition2 in devicetree._devices)
            self.assertFalse(self.vg in devicetree._devices)
            self.assertFalse(self.lv in devicetree._devices)

        flags.hide_unsupported_devices = False

    def test_initialize_disk(self):
        """ Test Blivet.initialize_disk with an unsupported disklabel. """
        devicetree = DeviceTree()
        devicetree._add_device(self.disk1)
        devicetree._add_device(self.partition1)
        devicetree._add_device(self.partition2)
        devicetree._add_device(self.disk2)
        devicetree._add_device(self.vg)
        devicetree._add_device(self.lv)

        b = Blivet()
        b.devicetree = devicetree
        with patch.object(b.devicetree, "recursive_remove") as recursive_remove:
            with patch("blivet.blivet.ActionCreateFormat") as ActionCreateFormat:
                with patch.multiple("blivet.blivet", _platform=DEFAULT, get_format=DEFAULT) as mocks:
                    mocks["get_format"].return_value = sentinel.new_fmt
                    b.initialize_disk(self.disk1)
                    recursive_remove.assert_called_with(self.disk1)
                    ActionCreateFormat.assert_called_with(self.disk1, fmt=sentinel.new_fmt)
