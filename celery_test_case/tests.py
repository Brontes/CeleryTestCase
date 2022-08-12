import datetime
import time

from celery.contrib.testing.worker import start_worker
from django.conf import settings
from django.core.cache import cache
from django.test import TransactionTestCase


def clear_cache():
    from django.core.cache import caches

    for k in settings.CACHES.keys():
        if k != 'session':
            caches[k].clear()


class DeviceTests(TransactionTestCase):
    celery_workers = []
    is_running = False
    beat_simulation_thread = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        clear_cache()
        cls.start_workers()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.terminate_workers()
        print(datetime.datetime.now(), "$$$ TearDownClass")

    @classmethod
    def start_workers(cls):
        from celery_test_case.tasks import QUEUE_SPECIAL, QUEUE_DEFAULT
        from CeleryTestCase.celery import app as celery_app
        celery_app.control.purge()
        for i in range(3):
            if i == 0:
                queue = QUEUE_SPECIAL
            else:
                queue = QUEUE_DEFAULT
            worker_generator = start_worker(celery_app, queues=queue, perform_ping_check=False)
            # noinspection PyUnresolvedReferences
            worker_generator.__enter__()
            cls.celery_workers.append(worker_generator)

    @classmethod
    def terminate_workers(cls):
        for worker_generator in cls.celery_workers:
            try:
                # noinspection PyUnresolvedReferences
                worker_generator.__exit__(None, None, None)
            except:
                import traceback
                print(datetime.datetime.now(), "$$$ Terminate workers error", traceback.format_exc())

    def celery_beat_simulation(self):
        import sys
        from celery_test_case.tasks import quick_task
        cnt = 0
        while self.is_running:
            print(datetime.datetime.now(), "$$$ Call quick_task")
            quick_task.apply_async((cnt,))
            time.sleep(.1)
            cnt += 1

        print(datetime.datetime.now(), "$$$ Exit beat simulation")
        sys.exit()

    def setUp(self) -> None:
        print(datetime.datetime.now(), f'$$$ {self._testMethodName} $$$ Start')
        super().setUp()

        # Celery beat simulator for testing purpose
        self.is_running = True
        import threading
        self.beat_simulation_thread = threading.Thread(target=self.celery_beat_simulation, daemon=True)
        self.beat_simulation_thread.start()

    def tearDown(self) -> None:
        self.is_running = False
        self.beat_simulation_thread.join()
        super().tearDown()
        print(f'*** {self._testMethodName} *** End', datetime.datetime.now())

    # noinspection PyMethodMayBeStatic
    async def test_multiple_queues(self):
        while cache.get("slow_app_runs") != 4:
            time.sleep(.1)
