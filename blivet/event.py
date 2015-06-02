# event.py
# Event management classes.
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

import abc
import copy
from collections import deque
from threading import RLock, Thread, current_thread
import pyudev
from six import add_metaclass
import time

from . import udev
from . import util
from .errors import EventManagerError, EventParamError, EventQueueEmptyError
from .flags import flags

import logging
event_log = logging.getLogger("blivet.event")

##
## Event
##
@add_metaclass(abc.ABCMeta)
class Event(util.ObjectID):
    """ An external event. """
    def __init__(self, action, info):
        """
            :param str action: a string describing the type of event
            :param info: information about the device

            The info parameter can be of any type provided that the subclass
            using it does so appropriately.
        """
        self.initialized = time.time()
        self.action = action
        self.info = info

    @abc.abstractproperty
    def device(self):
        """ Basename (friendly) of device this event acted upon. """
        return None

    def __str__(self):
        return "%s %s [%d]" % (self.action, self.device, self.id)

class UdevEvent(Event):
    """ A uevent. """
    def __init__(self, action, info):
        """
            :param str action: a string describing the type of event
            :param :class:`pyudev.Device` info: udev db entry
        """
        super(UdevEvent, self).__init__(action, info)

    @property
    def device(self):
        return udev.device_get_name(self.info)

class EventMask(util.ObjectID):
    """ Specification of events to ignore. """
    def __init__(self, device=None, action=None):
        """
            :keyword str device: basename of device to mask events on
            :keyword str action: action type to mask events of
        """
        self.device = device
        self.action = action

##
## EventQueue
##
class EventQueue(object):
    def __init__(self):
        self._queue = deque()
        self._lock = RLock()
        self._mask_list = []

    @property
    def queue(self):
        with self._lock:
            return self._queue

    def _mask_list_match(self, event):
        """ Return True if this event should be ignored """
        match = None
        for em in self._mask_list:
            if ((em.device is None or event.device == em.device) and
                (em.action is None or event.action == em.action)):
                match = em
                break

        return match

    def enqueue(self, event):
        with self._lock:
            if not self._mask_list_match(event):
                self._queue.append(event)

    def dequeue(self):
        """ Dequeue and return the next event.

            :returns: the next uevent
            :rtype: :class:`~.Event`
            :raises class:`~.errors.EventQueueEmptyError` if the queue is empty
        """
        with self._lock:
            if not self._queue:
                raise EventQueueEmptyError()

            return self._queue.popleft()

    def mask_add(self, device=None, action=None):
        """ Add an event mask and return the new :class:`EventMask`.

            :keyword str device: ignore events on the named device
            :keyword str action: ignore events of the specified type

            device of None means mask events on all devices
            action of None means mask all event types
        """
        em = EventMask(device=device, action=action)
        with self._lock:
            self._mask_list.append(em)
        return em

    def mask_remove(self, mask):
        try:
            with self._lock:
                self._mask_list.remove(mask)
        except ValueError:
            pass

    def __list__(self):
        return list(self._queue)

    def __iter__(self):
        return iter(self._queue)

    def __deepcopy__(self, memo):
        new = self.__class__.__new__(self.__class__)
        memo[id(self)] = new
        for (attr, value) in self.__dict__.items():
            if attr == "_lock":
                setattr(new, attr, RLock())
            else:
                setattr(new, attr, copy.deepcopy(value, memo))

        return new


##
## EventManager
##
class EventManager(object):
    _event_queue_class = EventQueue

    def __init__(self, handler_cb=None, notify_cb=None):
        self._handler_cb = None
        self._notify_cb = None

        if handler_cb is not None:
            self.handler_cb = handler_cb

        if notify_cb is not None:
            self.notify_cb = notify_cb

        self._queue = self._event_queue_class()

    #
    # handler_cb is the main event handler
    #
    def _set_handler_cb(self, cb):
        if not callable(cb):
            raise EventParamError("handler must be callable")

        self._handler_cb = cb

    def _get_handler_cb(self):
        return self._handler_cb

    handler_cb = property(lambda h: h._get_handler_cb(),
                          lambda h, cb: h._set_handler_cb(cb))

    #
    # notify_cb is a notification handler that runs after the main handler
    #
    def _set_notify_cb(self, cb):
        if not callable(cb) or cb.func_code.argcount < 1:
            raise EventParamError("callback function must accept at least one arg")

        self._notify_cb = cb

    def _get_notify_cb(self):
        return self._notify_cb

    notify_cb = property(lambda h: h._get_notify_cb(),
                         lambda h, cb: h._set_notify_cb(cb))

    @abc.abstractproperty
    def enabled(self):
        return False

    @abc.abstractmethod
    def enable(self):
        """ Enable monitoring and handling of events.

            :raises: :class:`~.errors.EventManagerError` if no callback defined
        """
        if self.handler_cb is None:
            raise EventManagerError("cannot enable handler with no callback")

    @abc.abstractmethod
    def disable(self):
        """ Disable monitoring and handling of events. """
        pass

    def mask_add(self, device=None, action=None):
        """ Add and return a new event mask.

            :keyword str device: ignore events on the named device
            :keyword str action: ignore events of the specified type
        """
        return self._queue.mask_add(device=device, action=action)

    def mask_remove(self, mask):
        return self._queue.mask_remove(mask)

    def next_event(self):
        return self._queue.dequeue()

    @property
    def events_pending(self):
        return len(self._queue.queue)

    @abc.abstractmethod
    def enqueue_event(self, *args, **kwargs):
        """ Convert event to :class:`Event`, enqueue it, then return it. """
        pass

    def handle_event(self, *args, **kwargs):
        """ Enqueue an event and call the configured handler. """
        event = self.enqueue_event(*args, **kwargs)
        t = Thread(target=self.handler_cb, name="Event%d" % event.id)
        t.daemon = True
        t.start()

class UdevEventManager(EventManager):
    def __init__(self, handler_cb=None, notify_cb=None):
        super(UdevEventManager, self).__init__(handler_cb=handler_cb,
                                               notify_cb=notify_cb)
        self._pyudev_observer = None

    def __deepcopy__(self, memo):
        return util.variable_copy(self, memo, shallow=('_pyudev_observer'))

    @property
    def enabled(self):
        return self._pyudev_observer and self._pyudev_observer.monitor.started

    def enable(self):
        """ Enable monitoring and handling of block device uevents. """
        event_log.info("enabling event handling")
        super(UdevEventManager, self).enable()
        monitor = pyudev.Monitor.from_netlink(udev.global_udev)
        monitor.filter_by("block")
        self._pyudev_observer = pyudev.MonitorObserver(monitor,
                                                       self.handle_event)
        self._pyudev_observer.start()
        flags.uevents = True

    def disable(self):
        """ Disable monitoring and handling of block device uevents. """
        event_log.info("disabling event handling")
        if self._pyudev_observer:
            self._pyudev_observer.stop()
            self._pyudev_observer = None

        self._queue.queue.clear()
        flags.uevents = False

    def __call__(self, *args, **kwargs):
        return self

    def enqueue_event(self, *args, **kwargs):
        event = UdevEvent(args[0], args[1])
        event_log.debug("-> %s", event)
        self._queue.enqueue(event)
        return event

    def handle_event(self, *args, **kwargs):
        """ Enqueue a uevent and call the configured handler. """
        _current_thread = current_thread()
        _current_thread.name = _current_thread.name.replace("Thread-",
                                                            "EventManager")
        super(UdevEventManager, self).handle_event(args[0], args[1])

eventManager = UdevEventManager()
