
import unittest

from blivet.event import Event, EventQueue

class FakeEvent(Event):
    @property
    def device(self):
        return self.info

class EventQueueTest(unittest.TestCase):
    def testEventQueue(self):
        q = EventQueue()

        # should be initially empty
        self.assertEqual(list(q), [])

        e1 = FakeEvent('add', 'sdc')
        q.enqueue(e1)
        self.assertEqual(list(q), [e1])

        e2 = FakeEvent('add', 'sdd')
        q.enqueue(e2)
        self.assertEqual(list(q), [e1, e2])

        e1d = q.dequeue()
        self.assertEqual(e1d, e1)
        self.assertEqual(list(q), [e2])

        mask = q.mask_add(device='sdc', action='change')
        e3 = FakeEvent('change', 'sdc')

        # should get enqueued since it isn't a blacklist match (action)
        e4 = FakeEvent('add', 'sdc')
        q.enqueue(e4)
        self.assertEqual(list(q), [e2, e4])

        # should get enqueued since it isn't a blacklist match (device)
        e5 = FakeEvent('change', 'sde')
        q.enqueue(e5)
        self.assertEqual(list(q), [e2, e4, e5])

        # enqueuing e3 should be a no-op
        q.enqueue(e3)
        self.assertEqual(list(q), [e2, e4, e5])

        q.mask_remove(mask)

        # now that the mask was removed, enqueuing e3 should work normally
        q.enqueue(e3)
        self.assertEqual(list(q), [e2, e4, e5, e3])

        # omitting device or action should mean "any device" or "any action",
        # respectively
        mask = q.mask_add(device='sdc')
        q.enqueue(e1)
        self.assertEqual(list(q), [e2, e4, e5, e3])
        q.enqueue(e1)
        self.assertEqual(list(q), [e2, e4, e5, e3])
        # verify that removing the mask works
        q.mask_remove(mask)
        q.enqueue(e1)
        self.assertEqual(list(q), [e2, e4, e5, e3, e1])

        mask = q.mask_add(action='remove')
        e6 = FakeEvent('remove', 'sdd2')
        # enqueue will be a no-op since event.action == "remove"
        q.enqueue(e6)
        self.assertEqual(list(q), [e2, e4, e5, e3, e1])
        # verify that the mask is gone after we tell the queue to remove it
        q.mask_remove(mask)
        q.enqueue(e6)
        self.assertEqual(list(q), [e2, e4, e5, e3, e1, e6])

        # shorten the queue to save some typing below
        _ignoreme = q.dequeue()
        _ignoreme = q.dequeue()
        _ignoreme = q.dequeue()
        _ignoreme = q.dequeue()
        self.assertEqual(list(q), [e1, e6])

if __name__ == "__main__":
    unittest.main()

