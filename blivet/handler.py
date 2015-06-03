# handler.py
# Event handling.
#
# Copyright (C) 2015  Red Hat, Inc.
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions of
# the GNU Lesser General Public License v.2, or (at your option) any later
# version. This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY expressed or implied, including the implied
# warranties of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See
# the GNU Lesser General Public License for more details.  You should have
# received a copy of the GNU Lesser General Public License along with this
# program; if not, write to the Free Software Foundation, Inc., 51 Franklin
# Street, Fifth Floor, Boston, MA 02110-1301, USA.  Any Red Hat trademarks
# that are incorporated in the source code or documentation are not subject
# to the GNU Lesser General Public License and may only be used or
# replicated with the express permission of Red Hat, Inc.
#
# Red Hat Author(s): David Lehman <dlehman@redhat.com>
#

import os
import re
import pprint
from six import add_metaclass

from . import udev
from .errors import DeviceError, EventQueueEmptyError
from .event import eventManager
from .formats import getFormat
from .storage_log import log_method_call
from .threads import SynchronizedMeta

import logging
log = logging.getLogger("blivet")
event_log = logging.getLogger("blivet.event")

@add_metaclass(SynchronizedMeta)
class EventHandler(object):
    def __init__(self, devicetree):
        self.devicetree = devicetree
        eventManager.handler_cb = self.handleUevent

    def handleUevent(self):
        """ Handle the next uevent in the queue. """
        log_method_call(self)
        if not eventManager.enabled:
            return

        try:
            event = eventManager.next_event()
        except EventQueueEmptyError:
            event_log.debug("uevent queue is empty")
            return

        event_log.debug("-- %s", event)
        if event.action == "add":
            self.deviceAddedCB(event)
        elif event.action == "remove":
            self.deviceRemovedCB(event)
        elif event.action == "change":
            self.deviceChangedCB(event)
        else:
            event_log.info("unknown event: %s", event)

        event_log.debug("<- %s", event)

    def deviceAddedCB(self, event, force=False):
        """ Handle an "add" uevent on a block device.

            The device could be newly created or newly activated.
        """
        info = event.info
        sysfs_path = udev.device_get_sysfs_path(info)
        log.debug("device added: %s", sysfs_path)
        if info.subsystem != "block":
            return

        if not info.is_initialized:
            log.debug("new device not initialized -- not processing it")
            return

        # add events are usually not meaningful for dm and md devices, but the
        # change event handler calls this method when a change event for such a
        # device appears to signal an addition
        # sometimes you get add events for md or dm that have no real info like
        # symbolic names -- ignore those, too.
        if not force and (udev.device_is_md(info) or
                          (udev.device_is_dm(info) or
                           re.match(r'/dev/dm-\d+$', info['DEVNAME']) or
                           re.match(r'/dev/md-\d+$', info['DEVNAME']))):
            log.debug("ignoring add event for %s", sysfs_path)
            return

        # If _syncBlivetOp returns True, this uevent is related to processing
        # of an action. It may or may not be in the tree.
        if self._syncBlivetOp(event):
            # This will update size, uuid, &c for new devices.
            return self.deviceChangedCB(event, expected=True)

        if self.devicetree.actions.processing:
            log.debug("ignoring unexpected event during action processing")
            return

        device = self.devicetree.getDeviceByName(udev.device_get_name(info))
        if device and device.exists:
            log.info("%s is already in the tree", udev.device_get_name(info))
            return

        # If we get here this should be a device that was added from outside of
        # blivet. Add it to the devicetree.
        device = self.devicetree._populator.addUdevDevice(info)
        if device:
            # if this device is on a hidden disk it should also be hidden
            self.devicetree._hideIgnoredDisks()

    def _diskLabelChangeHandler(self, info, device):
        log.info("checking for changes to disklabel on %s", device.name)
        # update the partition list
        device.format.updatePartedDisk()
        udev_devices = [d for d in udev.get_devices()
                if udev.device_get_disklabel_uuid(d) == device.format.uuid and
                    (udev.device_is_partition(d) or udev.device_is_dm_partition(d))]
        def udev_part_start(info):
            start = info.get("ID_PART_ENTRY_OFFSET")
            return int(start) if start is not None else start

        # remove any partitions we have that are no longer on disk
        remove = []
        for old in self.devicetree.getChildren(device):
            if not old.exists:
                log.warning("non-existent partition %s on changed "
                            "disklabel", old.name)
                remove.append(old)
                continue

            if old.isLogical:
                # msdos partitions are of the form
                # "%(disklabel_uuid)s-%(part_num)s". That's because
                # there's not any place to store an actual UUID in
                # the disklabel or partition metadata, I assume.
                # The reason this is so sad is that when you remove
                # logical partition that isn't the highest-numbered
                # one, the others all get their numbers shifted down
                # so the first one is always 5. Seriously. The msdos
                # partition UUIDs are pretty useless for logical
                # partitions.
                start = old.partedPartition.geometry.start
                new = next((p for p in udev_devices
                                if udev_part_start(info) == start),
                           None)
            else:
                new = next((p for p in udev_devices
                    if udev.device_get_partition_uuid(p) == old.uuid),
                            None)

            if new is None:
                log.info("partition %s was removed", old.name)
                remove.append(old)
            else:
                udev_devices.remove(new)

        if remove or udev_devices:
            self.devicetree.cancelDiskActions([device])

        for old in remove:
            if old not in self.devicetree.devices:
                # may have been removed by action cancelation
                continue

            self.devicetree.recursiveRemove(old, actions=False, modparent=False)

        # any partitions left in the list have been added
        for new in udev_devices:
            log.info("partition %s was added",
                     udev.device_get_name(new))
            self.devicetree._populator.addUdevDevice(new)

    def _getMemberUUID(self, info, device, container=False):
        uuid = udev.device_get_uuid(info)
        container_uuid = None
        if device.format.type == "btrfs":
            container_uuid = uuid
            uuid = info["ID_FS_UUID_SUB"]
        elif device.format.type == "mdmember":
            container_uuid = uuid
            uuid = udev.device_get_md_device_uuid(info)
        elif device.format.type == "lvmpv":
            # LVM doesn't put the VG UUID in udev
            if container:
                pv_info = self.devicetree.pvInfo.get(device.path)
                if pv_info is None:
                    log.error("no pv info available for %s", device.name)
                else:
                    container_uuid = pv_info.vg_uuid

        return uuid if not container else container_uuid

    def _getContainerUUID(self, info, device):
        return self._getMemberUUID(info, device, container=True)

    def _memberChangeHandler(self, info, device):
        """ Handle a change uevent on a container member device.

            :returns: whether the container changed
            :rtype: bool

        """
        if not hasattr(device.format, "containerUUID"):
            return

        uuid = self._getMemberUUID(info, device)
        container_uuid = self._getContainerUUID(info, device)

        old_container_uuid = device.format.containerUUID
        container_changed = (old_container_uuid != container_uuid)
        if container_changed:
            self.devicetree.cancelDiskActions(device.disks)

        try:
            container = self.devicetree.getChildren(device)[0]
        except IndexError:
            container = None

        if container and not container.exists:
            self.devicetree.cancelDiskActions(device.disks)

        if container_changed:
            if container:
                if len(container.parents) == 1:
                    self.devicetree.recursiveRemove(container, actions=False)
                else:
                    # FIXME: we need to be able to bypass the checks that
                    #        prevent ill-advised member removals
                    try:
                        container.parents.remove(device)
                    except DeviceError:
                        log.error("failed to remove %s from container %s "
                                  "to reflect uevent", device.name,
                                                       container.name)

            device.format = None
            self.devicetree._populator.handleUdevDeviceFormat(info, device)
        else:
            device.format.containerUUID = container_uuid
            device.format.uuid = uuid

            if device.format.type == "lvmpv":
                pv_info = self.devicetree.pvInfo.get(device.path)
                # vg rename
                device.format.vgName = pv_info.vg_name
                device.format.peStart = pv_info.pe_start
                if container:
                    container.name = pv_info.vg_name
                    self.devicetree._populator.updateLVs(container)

        # MD TODO: raid level, spares
        # BTRFS TODO: check for changes to subvol list IFF the volume is mounted

    def _update_uuids(self, event, device):
        """ Set or update UUID of format and, as appropriate, container.

            This method is called from :meth:`_syncBlivetOp` and from
            :meth:`deviceChangedCB`.
        """
        if device.partitionable and event.info.get("ID_PART_TABLE_TYPE"):
            uuid = udev.device_get_disklabel_uuid(event.info)
        else:
            # this works for regular filesystems as well as container members
            uuid = self._getMemberUUID(event.info, device)

        log.info("old uuid: %s ; new uuid: %s", device.format.uuid, uuid)
        device.format.uuid = uuid
        if hasattr(device.format, "containerUUID"):
            if device.format.type == "lvmpv":
                self.devicetree.dropLVMCache()
            device.format.containerUUID = self._getContainerUUID(event.info,
                                                                 device)
            try:
                container = self.devicetree.getChildren(device)[0]
            except IndexError:
                pass
            else:
                container.uuid = device.format.containerUUID

    def deviceChangedCB(self, event, expected=False):
        """ Handle a "changed" uevent on a block device. """
        info = event.info
        sysfs_path = udev.device_get_sysfs_path(info)
        log.debug("device changed: %s", sysfs_path)
        if info.subsystem != "block":
            return

        if not info.is_initialized:
            log.debug("new device not initialized -- not processing it")
            return

        name = udev.device_get_name(info)
        if name.startswith("temporary-cryptsetup-"):
            return

        # If we do the lookup by name here the lookup will fail if the device
        # has just been renamed.
        # If we do the lookup by sysfs path the lookup will fail if the device
        # has just been activate from outside blivet.
        device = self.devicetree.getDeviceBySysfsPath(sysfs_path, hidden=True)
        if not device:
            device = self.devicetree.getDeviceByName(name, hidden=True)
            if device and not device.sysfsPath and device.status:
                # If the device was activated from outside of blivet we have to
                # update the sysfs path now. Otherwise, at the very least, we
                # won't be able to determine its size.
                device.sysfsPath = sysfs_path

        if (not expected and not device and
            ((udev.device_is_md(info) and "MD_UUID" in info) or
             (udev.device_is_dm(info) and "DM_NAME" in info))):
            # md and dm devices aren't really added until you get a change
            # event
            return self.deviceAddedCB(event, force=True)

        if not expected:
            # See if this event was triggered a blivet action.
            expected = self._syncBlivetOp(event)

        if device and not device.exists:
            log.error("aborting event handler for non-existent device")
            return
            self.devicetree.cancelDiskActions(device.disks)
            # now try again to look up the device
            device = self.devicetree.getDeviceBySysfsPath(sysfs_path,
                                                          hidden=True)
            if not device:
                device = self.devicetree.getDeviceByName(name, hidden=True)

        if not expected and self.devicetree.actions.processing:
            log.debug("ignoring unexpected event during action processing")
            return

        if not device:
            # We're not concerned with updating devices that have been removed
            # from the tree.
            log.info("device not found: %s", udev.device_get_name(info))
            return

        ##
        ## Check for changes to the device itself.
        ##

        # rename
        name = info.get("DM_LV_NAME", udev.device_get_name(info))
        if getattr(device, "lvname", "name") != name:
            device.name = name

        if not os.path.exists(device.path):
            log.info("ignoring change uevent on device with no node (%s)", device.path)
            return

        # resize
        # XXX resize of inactive lvs is handled in updateLVs (via change event
        #     handler for pv(s))
        current_size = device.readCurrentSize()
        if expected or device.currentSize != current_size:
            if not expected:
                self.devicetree.cancelDiskActions(device.disks)
            device.updateSize(newsize=current_size)
            # FIXME: update fs size here?

        if not expected and not device.format.exists:
            self.devicetree.cancelDiskActions(device.disks)

        log.debug("changed: %s", pprint.pformat(dict(info)))

        ##
        ## Handle changes to the data it contains.
        ##
        partitioned = device.partitionable and info.get("ID_PART_TABLE_TYPE")
        if partitioned:
            uuid = udev.device_get_disklabel_uuid(info)
        else:
            uuid = self._getMemberUUID(info, device)

        label = udev.device_get_label(info)

        new_type = getFormat(udev.device_get_format(info)).type
        type_changed = (new_type != device.format.type and
                        not
                        (device.format.type == "disklabel" and partitioned))
        uuid_changed = (device.format.uuid and device.format.uuid != uuid)
        reformatted = uuid_changed or type_changed
        log.info("partitioned: %s\ntype_changed: %s\nold type: %s\nnew type: %s",
                 partitioned, type_changed, device.format.type, new_type)
        log.info("old uuid: %s ; new uuid: %s", device.format.uuid, uuid)

        if not type_changed:
            if hasattr(device.format, "label"):
                device.format.label = label

            if not expected:
                self._update_uuids(event, device)

        if expected:
            return

        if reformatted:
            log.info("%s was reformatted from outside of blivet", device.name)
            self.devicetree.cancelDiskActions(device.disks)
            # eg: wipefs on lvm pv w/ configured vg and lvs
            for child in self.devicetree.getChildren(device):
                self.devicetree.recursiveRemove(child, actions=False)

            device.format = None
            self.devicetree._populator.handleUdevDeviceFormat(info, device)
        elif partitioned:
            self._diskLabelChangeHandler(info, device)
        elif hasattr(device.format, "containerUUID"):
            if device.format.type == "lvmpv":
                self.devicetree.dropLVMCache()

            self._memberChangeHandler(info, device)

    def deviceRemovedCB(self, event):
        """ Handle a "remove" uevent on a block device.

            This is generally going to be interpreted as a deactivation as
            opposed to a removal since there is no consistent way to determine
            which it is from the information given.

            It seems sensible to interpret remove events as deactivations and
            handle destruction via change events on parent devices.
        """
        info = event.info
        log.debug("device removed: %s", udev.device_get_sysfs_path(info))
        if info.subsystem != "block":
            return

        if self._syncBlivetOp(event):
            return

        if self.devicetree.actions.processing:
            log.debug("ignoring unexpected event during action processing")
            return

        # XXX Don't forget about disks actually going offline for some reason.

        device = self.devicetree.getDeviceByName(udev.device_get_name(info))
        if device:
            device.sysfsPath = ""

            # update FS instances since the device is surely no longer mounted
            for fmt in (device.format, device.originalFormat):
                if hasattr(fmt, "_mountpoint"):
                    fmt._mountpoint = None
                    fmt._mounted_read_only = False

    def _look_up_device(self, info=None, devices=None):
        if info is None or devices is None:
            return None

        name = udev.device_get_name(info)

        # We can't do this lookup by sysfs path since the StorageDevice
        # might have just been created, in which case it might not have a
        # meaningful sysfs path (for dm and md they aren't predictable).
        device = None
        for _device in devices:
            # If the device, it's format, or it's original format has an active
            # event sync that matches the event, return it.
            #
            # We should never associate events with sync sets.
            #
            # This is part of why it is important to activate and deactivate
            # event sync flags as near as possible to the actual operation.

            if _device.name != name:
                # XXX md devices sometimes have no symbolic name by the time
                #     they are removed, so we have to look it up by sysfs
                #     path
                if not (name.startswith("md") and
                        _device.sysfsPath and
                        name == os.path.basename(_device.sysfsPath)):
                    continue

            event_syncs = [_device.modifySync, _device.controlSync,
                           _device.format.eventSync,
                           _device.originalFormat.eventSync]
            if any(es.awaiting_sync for es in event_syncs if not es.aggregate):
                device = _device
                break

        return device

    def _syncBlivetOp(self, event):
        """ Confirm completion of a blivet operation using an event.

            :param event: an external event
            :type event: :class:`~.event.Event`
            :returns: True if event corresponds to a blivet-initiated operation
            :rtype: bool

            Methods :meth:`~.devices.StorageDevice.create`,
            :meth:`~.devices.StorageDevice.destroy`, and
            :meth:`~.devices.StorageDevice.setup` all use a synchronization
            manager (:class:`~.synchronizer.EventSynchronizer`) to
            synchronize the finalization/confirmation of their respective
            operations. Flags within the manager are used to indicate which, if
            any, of these methods is under way.

            Generally, the goal is to associate an event with the operation
            that caused it. It is possible that an event is not associated with
            any blivet operation. It is also possible that an event is
            associated with a device that is no longer in the devicetree.
        """
        name = udev.device_get_name(event.info)
        log_method_call(self, name=name, action=event.action,
                        sysfs_path=udev.device_get_sysfs_path(event.info))

        # Get a list of all device instances.
        devices = self.devicetree.devices
        devices.extend(a.device for a in self.devicetree.actions.find()
                                    if a.device not in devices)

        ## If we can't associate this event with a device return now.
        device = self._look_up_device(info=event.info, devices=devices)
        if device is None:
            return False

        log.debug("event device is '%s'", device)

        ## Try to associate the event with an event sync.
        event_sync = None
        if (event.action in ("add", "change") and
            device.modifySync.awaiting_sync and
            device.modifySync.creating):
            ## device create without event delegation (eg: partition)
            event_log.debug("* create %s", device.name)
            event_sync = device.modifySync
        elif (event.action in ("add", "change") and
              device.controlSync.awaiting_sync and
              device.controlSync.starting):
            ## device setup
            event_log.debug("* setup %s", device.name)
            # update sysfsPath since the device will not have access to it
            # until later in the change handler
            device.sysfsPath = udev.device_get_sysfs_path(event.info)
            event_sync = device.controlSync
        elif (event.action in ("add", "change") and
              device.controlSync.awaiting_sync and
              device.controlSync.stopping and
              device.name.startswith("loop")):
            ## loop device teardown
            # XXX You don't get a remove event when you deactivate a loop
            #     device.
            event_log.debug("* teardown %s", device.name)
            event_sync = device.controlSync
        elif (event.action == "change" and
              device.modifySync.awaiting_sync and
              device.modifySync.resizing):
            ## device resize
            event_log.debug("* resize %s", device.name)
            event_sync = device.modifySync
        elif (event.action == "change" and 
              device.controlSync.awaiting_sync and
              device.controlSync.changing and
              device.controlSync.validate(event)):
            ## device change (eg: event on pv for vg or lv creation)
            event_log.debug("* change %s", device.name)
            event_sync = device.controlSync
        elif (event.action == "change" and
              device.format.eventSync.awaiting_sync and
              device.format.eventSync.validate(event)):
            ## any change to a format
            event_log.debug("* change %s format", device.name)
            event_sync = device.format.eventSync
        elif (event.action == "change" and
              device.originalFormat.eventSync.awaiting_sync and
              device.originalFormat.eventSync.validate(event)):
            ## any change to a format
            event_log.debug("* change %s format", device.name)
            event_sync = device.originalFormat.eventSync
        elif (event.action == "remove" and
              device.controlSync.awaiting_sync and
              device.controlSync.stopping):
            ## device teardown
            event_log.debug("* teardown %s", device.name)
            event_sync = device.controlSync
        elif (event.action == "remove" and
              device.modifySync.awaiting_sync and
              device.modifySync.destroying):
            ## device destroy
            event_log.debug("* destroy %s", device.name)
            event_sync = device.modifySync

        ret = False
        if event_sync is not None:
            event_sync.matched = True
            event_log.debug("waiting for ready %s", name)
            event_sync.wait_for_ready()
            event_log.debug("notify %s", name)
            if event_sync.creating or event_sync.changing:
                # Set UUIDs now for newly-created devices and formats.
                self._update_uuids(event, device)
            event_sync.notify()
            event_log.debug("wait %s", name)
            event_sync.wait()
            event_log.debug("done synchronizing %s", name)
            ret = True

        return ret
