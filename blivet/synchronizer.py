# synchronizer.py
# Synchronization between blivet and udev.
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

import threading
import copy
import pprint
import time

from .errors import SynchronizationError
from .flags import flags
from .threads import blivet_lock
from . import util

import logging
log = logging.getLogger("blivet")
event_log = logging.getLogger("blivet.event")

KEY_PRESENT = 255
KEY_ABSENT = 256

"""
    flags
        timestamp (set when a flag is set)

    filter
        timestamp (used to match events)

    synchronizer
        flags
        filter
        cv
        event
"""

class OpFlags(util.ObjectID):
    """ A set of boolean flags, each of which represents a class of storage
        operation. Only one flag may be active at a time.
    """

    flag_names = ["starting", "stopping", "creating", "destroying", "resizing",
                  "changing"]

    def __init__(self):
        self.reset()

    @property
    def active(self):
        return any(self._flags.values())

    def reset(self):
        # pylint: disable=attribute-defined-outside-init
        self._flags = {flag_name: False for flag_name in self.flag_names}

    def _get_flag(self, flag):
        return self._flags[flag]

    def _set_flag(self, flag, val):
        if val and self.active:
            active = next((f for f,v in self._flags.items() if v is True), None)
            log.error("%s is already active", active)
            raise SynchronizationError("only one flag can be active at a time")

        self._flags[flag] = val

    starting = property(lambda s: s._get_flag("starting"),
                        lambda s,v: s._set_flag("starting", v))
    stopping = property(lambda s: s._get_flag("stopping"),
                        lambda s,v: s._set_flag("stopping", v))
    creating = property(lambda s: s._get_flag("creating"),
                        lambda s,v: s._set_flag("creating", v))
    destroying = property(lambda s: s._get_flag("destroying"),
                        lambda s,v: s._set_flag("destroying", v))
    resizing = property(lambda s: s._get_flag("resizing"),
                        lambda s,v: s._set_flag("resizing", v))
    changing = property(lambda s: s._get_flag("changing"),
                        lambda s,v: s._set_flag("changing", v))

class OpEventValidator(object):
    """ A simple info filter framework, the purpose of which is to provide a
        way for event handlers to correctly associate received events with
        blivet-initiated operations.
    """
    def __init__(self):
        self.reset()

    def reset(self):
        # pylint: disable=attribute-defined-outside-init
        self._required_info = dict()

        # pylint: disable=attribute-defined-outside-init
        self.timestamp = 0

    def update_requirements(self, *args, **kwargs):
        """ Update requirements dict.

            Items are dictionary key/value pairs, where the value can be either
            a specific value or one of the constants KEY_ABSENT or KEY_PRESENT.
        """
        self._required_info.update(*args, **kwargs)

    def remove_requirement(self, key):
        """ Remove an entry from the requirements dict by key. """
        if key in self._required_info:
            del self._required_info[key]

    def validate(self, event):
        """ Return True if the event satisfies the requirements.

            :keyword event: the event we're examining
            :type event: :class:`~.event.Event`
            :returns: whether the event satisfied the requirements
            :rtype: bool
        """
        info = event.info
        log.debug("validating requirements %s",
                  pprint.pformat(self._required_info))
        log.debug("event info: %s", pprint.pformat(dict(info)))
        if event.initialized < self.timestamp:
            log.debug("returning False due to timestamps")
            return False

        valid = True
        for (key, value) in self._required_info.items():
            if value == KEY_ABSENT and key in info:
                valid = False
                break
            elif value == KEY_PRESENT and key not in info:
                valid = False
                break
            elif value not in (KEY_ABSENT, KEY_PRESENT) and \
                 (key not in info or info[key] != value):
                valid = False
                break

        log.debug("returning %s", valid)
        return valid

class OpEventSyncBase(util.ObjectID):
    """ Base class for synchronizing blivet operations with external events. """
    def __init__(self, passthrough=False):
        """
            :keyword bool passthrough: True if synchronization should be a no-op
        """
        self.passthrough = passthrough
        """ If True, this instance doesn't wait on or notify other threads. """

        self.matched = False

    #
    # state
    #
    def reset(self):
        """ Reset this instance. """
        self.clear_ready()

        # pylint: disable=attribute-defined-outside-init
        self.timestamp = 0

        self.matched = False

    @property
    def active(self):
        """ Is an operation in progress that uses this instance? """
        return False

    @property
    def awaiting_sync(self):
        return self.active and not self.aggregate and not self.matched

    @property
    def aggregate(self):
        """ Does this event sync merely wrap a set of other event syncs? """
        return False

    #
    # control passing
    #
    def wait(self, timeout=None):
        """ Wait until notified or until a timeout occurs.

            .. note::

                See :meth:`threading.Condition.wait`.

        """
        pass

    def notify(self):
        """ Wake up one thread that is waiting on this instance.

            .. note::

                See :meth:`threading.Condition.notify`.

        """
        pass

    #
    # initial sync-up
    #
    def wait_for_ready(self):
        """ Wait until this instance is ready to begin synchronization. """
        pass

    def ready_wait(self, timeout=None):
        pass

    def set_ready(self):
        """ Set this instance as ready to begin synchronization. """
        pass

    def clear_ready(self):
        """ Set this instance as not ready to begin synchronization. """
        pass

    #
    # flags
    #
    # pylint: disable=unused-argument
    def _get_flag(self, flag):
        pass

    # pylint: disable=unused-argument
    def _set_flag(self, flag, val):
        self.timestamp = time.time()

    starting = property(lambda s: s._get_flag("starting"),
                        lambda s,v: s._set_flag("starting", v))
    stopping = property(lambda s: s._get_flag("stopping"),
                        lambda s,v: s._set_flag("stopping", v))
    creating = property(lambda s: s._get_flag("creating"),
                        lambda s,v: s._set_flag("creating", v))
    destroying = property(lambda s: s._get_flag("destroying"),
                        lambda s,v: s._set_flag("destroying", v))
    resizing = property(lambda s: s._get_flag("resizing"),
                        lambda s,v: s._set_flag("resizing", v))
    changing = property(lambda s: s._get_flag("changing"),
                        lambda s,v: s._set_flag("changing", v))

    #
    # event validation
    #
    def update_requirements(self, *args, **kwargs):
        pass

    def remove_requirement(self, key):
        pass

    def validate(self, event):
        pass


class OpEventSync(OpEventSyncBase):
    """ Manager for shared state related to storage operations.

        Each :class:`~.devices.StorageDevice` and
        :class:`~.formats.DeviceFormat` instance contains an instance of this
        class. It is used by uevent handlers to notify
        :class:`~.devices.StorageDevice` or :class:`~.formats.DeviceFormat`
        instances when operations like create, destroy, setup, teardown have
        completed.

        This class internally uses a :class:`threading.Condition` and a
        :class:`threading.Event` to achieve synchronization between event
        handler threads and the main blivet thread. The :class:`threading.Event`
        is used to coordinate the initial rendezvous, while the
        :class:`threading.Condition` is used to pass control back and forth
        between an event thread and the main blivet thread while performing
        actual synchronization. The basic model is as follows:

            "main.1" is the first step in the main thread.
            "event.1" is the first step in the associated event thread.

            <main.1> perform operation
            <main.2> mark synchronizer ready
            <main.3> wait for notification from event handler of event receipt
            <main.4> do accounting (eg: for destroy, set self.exists to False)
            <main.5> notify event thread that accounting is complete
            <main.6> continue with execution

            <event.1> receive event
            <event.2> associate synchronizer with event
            <event.3> wait until synchronizer is ready
            <event.4> notify main thread that event has been received
            <event.5> wait until main thread has finished accounting
            <event.6> continue with execution

        Without main.2 and event.3 it is impossible to predict with certainty
        which of main.3 or event.3 will occur first. The
        :class:`threading.Event` manages this initial sync-up nicely.
    """
    def __init__(self, passthrough=False):
        super(OpEventSync, self).__init__(passthrough=passthrough)

        self._flags = OpFlags()
        """ Flags indicating what type of operation we are confirming. """

        self._validator = OpEventValidator()
        """ Validation of potential matching events. """

        self._ready = threading.Event()
        """ Set when code that initiated the op is ready to synchronize. """

        self._cv = threading.Condition(blivet_lock)
        """ Used to pass control back and forth during synchronization. """

    def __deepcopy__(self, memo):
        new = self.__class__.__new__(self.__class__)
        memo[id(self)] = new
        for (attr, value) in self.__dict__.items():
            if attr == "_cv":
                setattr(new, attr,threading.Condition(blivet_lock))
            elif attr == "_ready":
                setattr(new, attr, threading.Event())
            else:
                setattr(new, attr, copy.deepcopy(value, memo))

        return new

    #
    # state
    #
    def reset(self):
        log.debug("resetting event sync %d", self.id)
        super(OpEventSync, self).reset()
        self._flags.reset()
        self._validator.reset()

    @property
    def active(self):
        log.debug("event sync %d flags: %s", self.id, self._flags._flags)
        return self._flags.active

    #
    # control passing
    #
    def wait(self, timeout=None, unready=False):
        if self.passthrough or not flags.uevents:
            return

        if not unready and not self._ready.is_set():
            raise SynchronizationError("wait called before sync is ready")

        args = [] if timeout is None else [timeout]
        return self._cv.wait(*args)

    def notify(self):
        if self.passthrough or not flags.uevents:
            return

        if not self._ready.is_set():
            raise SynchronizationError("notify called before sync is ready")

        log.debug("notify %d", self.id)
        return self._cv.notify()

    #
    # initial sync-up
    #
    def wait_for_ready(self):
        if self.passthrough or not flags.uevents:
            return

        while not self._ready.is_set():
            # By waiting on the Condition here we get the threading module to
            # do a _release_save of the global lock so other threads can
            # proceed and then _acquire_restore to get the lock back to check
            # the Event again.
            self.wait(timeout=0.1, unready=True)

    def ready_wait(self, timeout=None):
        event_log.debug("setting %d ready", self.id)
        self.set_ready()
        self.wait(timeout=timeout)

    def set_ready(self):
        self._ready.set()

    def clear_ready(self):
        self._ready.clear()

    #
    # flags
    #
    def _get_flag(self, flag):
        return getattr(self._flags, flag)

    def _set_flag(self, flag, val):
        super(OpEventSync, self)._set_flag(flag, val)
        self._validator.timestamp = self.timestamp
        event_log.debug("setting sync %d %s to %s", self.id, flag, val)
        return setattr(self._flags, flag, val)

    #
    # event validation
    #
    def update_requirements(self, *args, **kwargs):
        return self._validator.update_requirements(*args, **kwargs)

    def remove_requirement(self, key):
        return self._validator.remove_requirement(key)

    def validate(self, event):
        valid = self._validator.validate(event)
        self.matched |= valid
        return valid

class OpEventSyncSet(OpEventSyncBase):
    """ Aggregates a set of :class:`OpEventSync` instances.

        This is for :class:`~.devices.ContainerDevice` subclasses, which
        generally rely on events on the member devices for some operations.
    """
    def __init__(self, passthrough=False, members=None):
        super(OpEventSyncSet, self).__init__(passthrough=passthrough)
        self._members = members

    def _foreach_method_call(self, method, *args, **kwargs):
        for member in self._members:
            getattr(member, method)(*args, **kwargs)

    #
    # state
    #
    def reset(self):
        super(OpEventSyncSet, self).reset()
        self._foreach_method_call('reset')

    @property
    def active(self):
        return any(m.active for m in self._members)

    @property
    def aggregate(self):
        return True

    #
    # flags
    #
    def _get_flag(self, flag):
        # Any operation on a container manifests as a change event on members.
        flag = "changing"

        # if the flag is active for any member it is active for the set
        return any(getattr(m, flag) for m in self._members)

    def _set_flag(self, flag, val):
        # Any operation on a container manifests as a change event on members.
        flag = "changing"

        # Only allow setting a flag active if no other flag is active for any
        # member.
        if val is True:
            active = next((getattr(self, f) for f in OpFlags.flag_names if f != flag), None)
            if active:
                log.error("flag %s is already active", active)
                raise SynchronizationError("cannot have multiple active flags")

        for member in self._members:
            setattr(member, flag, val)

    #
    # control passing
    #
    def wait(self, timeout=None):
        self._foreach_method_call('wait', timeout=timeout)

    def notify(self):
        self._foreach_method_call('notify')

    #
    # initial sync-up
    #
    def wait_for_ready(self):
        self._foreach_method_call('wait_for_ready')

    def ready_wait(self, timeout=None):
        self._foreach_method_call('ready_wait', timeout=timeout)

    def clear_ready(self):
        self._foreach_method_call('clear_ready')

    def set_ready(self):
        self._foreach_method_call('set_ready')

    #
    # event validation
    #
    def update_requirements(self, *args, **kwargs):
        self._foreach_method_call('update_requirements', *args, **kwargs)

    def remove_requirement(self, key):
        self._foreach_method_call('remove_requirement', key)

    def validate(self, event):
        raise SynchronizationError("OpEventSyncSet.validate called directly")
