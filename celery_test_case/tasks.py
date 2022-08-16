import datetime
import threading
import time

from django.core.cache import cache

from celery import shared_task
from celery.contrib.abortable import AbortableTask
from celery.signals import (
    import_modules, task_failure, task_internal_error, task_postrun, task_prerun, task_received, task_rejected,
    task_retry, task_revoked, task_sent, task_success, task_unknown, worker_init, worker_process_init,
    worker_process_shutdown, worker_ready, worker_shutdown, worker_shutting_down
)

QUEUE_DEFAULT = 'celery'
QUEUE_SPECIAL = 'special'


@shared_task(bind=True, base=AbortableTask)
def quick_task(self, cnt):
    print(datetime.datetime.now(), "### quick_task - start")
    time.sleep(.05)
    if cnt % 13 == 0:
        print(datetime.datetime.now(), "&&& pre apply slow_task")
        slow_task.apply_async()
        print(datetime.datetime.now(), "&&& post apply slow_task")
    if cnt % 10 == 0:
        print(datetime.datetime.now(), "&&& pre apply special_task")
        special_task.apply_async()
        print(datetime.datetime.now(), "&&& post apply special_task")
    print(datetime.datetime.now(), "### quick_task - end")


@shared_task(bind=True, base=AbortableTask)
def slow_task(self):
    print(datetime.datetime.now(), "### slow_task - start")
    time.sleep(3)
    print(datetime.datetime.now(), "### slow_task - end")
    cache.incr("slow_app_runs", ignore_key_check=True)


@shared_task(bind=True, base=AbortableTask)
def special_task(self):
    print(datetime.datetime.now(), "### special_task - start")
    time.sleep(1)
    print(datetime.datetime.now(), "### special_task - end")


@task_prerun.connect
@task_postrun.connect
def task_run_handler(signal, **kwargs):
    sender = kwargs.get('sender', None)

    sender_name = sender.name if sender else 'unknown'

    print(datetime.datetime.now(), f"***Task event: Thread: {threading.current_thread()}, "
                                   f"Sender: {sender_name}, Task id: {kwargs.get('task_id', '')}, "
                                   f"Signal: {signal.name}, kwargs: {kwargs}")


@task_received.connect
@task_success.connect
@task_retry.connect
@task_failure.connect
@task_internal_error.connect
@task_revoked.connect
@task_rejected.connect
@task_unknown.connect
@task_sent.connect
@import_modules.connect
@worker_init.connect
@worker_process_init.connect
@worker_process_shutdown.connect
@worker_ready.connect
@worker_shutdown.connect
@worker_shutting_down.connect
def task_logging(signal, **kwargs):
    print(datetime.datetime.now(), f"***Task logging: Signal: {signal.name}, kwargs: {kwargs}")
