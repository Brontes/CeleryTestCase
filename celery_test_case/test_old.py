import asyncio
import datetime
import json
import time
from datetime import timedelta

from asgiref.sync import sync_to_async
from celery.contrib.testing.worker import start_worker
from channels.routing import URLRouter
from channels.testing import WebsocketCommunicator
from django import db
from django.conf import settings
from django.core import mail
from django.http import Http404
from django.test import TransactionTestCase
from django.utils import timezone

import device
from device.api.command import Command
from device.api.command_handling import get_command, get_command_id, get_command_timestamp
from device.api.device import Device
from device.models import Command as CommandModel, CommandStatus, Device as DeviceModel, DeviceState, Log, LogState
from device.routing import routeDeviceConsumer
from device.utils import epoch_milliseconds, json_datetime
from settings.device.command import Commands
from settings.device.device import SCDeviceType

DEVICE_TOKEN = '648e4010-2db5-11ec-8d3d-0242ac130003'


def clear_records():
    Log.objects.all().delete()
    CommandModel.objects.all().delete()
    DeviceModel.objects.all().delete()


def insert_device():
    return Device.create_device(token=DEVICE_TOKEN,
                                identifier='11:22:33:44:55:66',
                                type=SCDeviceType.AT7_V2,
                                hw_version=SCDeviceType.AT7_V2.description,
                                activation_at=timezone.now(),
                                latest_ping=timezone.now(),
                                state=DeviceState.REGISTERED,
                                reporting=True).device


def add_command(command_type=Commands.TEST_OUTGOING_LOG, params=None):
    if params is None:
        params = {}
    params['device'] = Device.init_device(DEVICE_TOKEN).device
    return Command.get_command_object(command_type)().add_to_queue(**params)


async def add_command_async(command_type=Commands.TEST_OUTGOING_LOG, params=None):
    async_func = sync_to_async(add_command, thread_sensitive=True)
    return await async_func(command_type, params)


async def communicator_send_to(communicator, text_data=None, bytes_data=None):
    print("***Communicator send", communicator)
    return await communicator.send_to(text_data, bytes_data)


async def send_response(communicator, cmd_id, status, data=None, params=None, session_id=None):
    response = dict(response_cmd_id=cmd_id, status=status, _sent_timestamp=epoch_milliseconds())
    if data is not None:
        response['result'] = data
    if params is not None:
        response['params'] = params
    if session_id is not None:
        response['_session_id'] = session_id
    await communicator_send_to(communicator, text_data=json.dumps(response))


async def send_request(communicator, cmd, params, unique_id, expire_at, session_id):
    request = dict(cmd=cmd, params=params, cmd_id=unique_id, expire_at=json_datetime(expire_at),
                   _sent_timestamp=epoch_milliseconds(), _session_id=session_id)
    if cmd is None:
        request.pop('cmd')
    await communicator_send_to(communicator, text_data=json.dumps(request))


def close_db():
    db.connection.close()


def get_command_record(cmd_unique_id, device_id=None):
    time.sleep(.2)
    if device_id:
        cmd = get_command(device_id=device_id, device_cmd_id=get_command_id(cmd_unique_id))
    else:
        cmd = get_command(pk=get_command_id(cmd_unique_id))

    # Zato, da se naloada executed by v sync funkciji... To potrebujem pri preverjanju loga, ki je async
    # noinspection PyUnusedLocal
    tmp = cmd.executed_by.id if cmd.executed_by else 0  # noqa
    return cmd


async def get_command_record_async(cmd_unique_id, device_id=None):
    async_func = sync_to_async(get_command_record, thread_sensitive=True)
    return await async_func(cmd_unique_id, device_id)


def get_command_object(cmd_unique_id, device_id=None):
    return Command.init_command_object(get_command_record(cmd_unique_id, device_id))


async def get_command_object_async(cmd_unique_id, device_id=None):
    async_func = sync_to_async(get_command_object, thread_sensitive=True)
    return await async_func(cmd_unique_id, device_id)


def update_command_record(cmd_unique_id, device_id=None, update_values=None):
    cmd = get_command_record(cmd_unique_id, device_id)
    for field, value in update_values.items():
        setattr(cmd, field, value)
    cmd.save()


async def update_command_record_async(cmd_unique_id, device_id=None, update_values=None):
    async_func = sync_to_async(update_command_record, thread_sensitive=True)
    return await async_func(cmd_unique_id, device_id, update_values)


def update_device_record(device_token, update_values=None):
    device_record = Device.init_device(device_token).device
    for field, value in update_values.items():
        setattr(device_record, field, value)
    device_record.save()


async def update_device_record_async(device_token, update_values=None):
    async_func = sync_to_async(update_device_record, thread_sensitive=True)
    return await async_func(device_token, update_values)


def get_log_record(cmd, log=None):
    if log is False:
        log = Log.objects.order_by('id').last()
    elif log:
        log.refresh_from_db()
    else:
        log = Log.objects.filter(command=cmd.cmd).first()
    return log


async def get_log_record_async(cmd, log=None):
    async_func = sync_to_async(get_log_record, thread_sensitive=True)
    return await async_func(cmd, log)


def get_log_record_count():
    return Log.objects.count()


async def get_log_record_count_async():
    async_func = sync_to_async(get_log_record_count, thread_sensitive=True)
    return await async_func()


def clear_cache():
    from django.core.cache import caches

    for k in settings.CACHES.keys():
        if k != 'session':
            caches[k].clear()


class DeviceTests(TransactionTestCase):
    celery_workers = []
    is_running = False
    communicator = None
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
        print("*** TearDownClass", datetime.datetime.now())

    @classmethod
    def start_workers(cls):
        from device.tasks import QUEUE_CANCEL, QUEUE_DEFAULT
        # TODO: DC - Ne sme biti povezave na scantron server...
        #  Pomeni, da bo treba tudi testne komande prestaviti iz Scantron server v device...
        #  In narediti default celery app v device conceptu?
        from ScantronServer.celery import app as celery_app, app_cancel as cancel_app
        celery_app.control.purge()
        print("###Worker apps", celery_app, cancel_app)
        # Dva workerja sta potrebna za test cancelanja taskov.
        # Zato ker en worker teče da obedeluje nek task, drugi worker pa potem rihta cancel komando.
        for i in range(3):
            if i == 0:
                queue = QUEUE_CANCEL
                app = cancel_app
            else:
                queue = QUEUE_DEFAULT
                app = celery_app
            worker_generator = start_worker(app, queues=queue, perform_ping_check=False)
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
                print("*** Terminate workers error", datetime.datetime.now(), traceback.format_exc())
                pass

    def celery_beat_simulation(self):
        import sys
        from ScantronServer.celery import call_device_command_handler
        while self.is_running:
            print("*** Call command handler", datetime.datetime.now())
            call_device_command_handler.apply_async()
            time.sleep(1)

        print("*** Exit beat simulation", datetime.datetime.now())
        db.connection.close()
        sys.exit()

    async def connect_to_ws(self):
        application = URLRouter([routeDeviceConsumer, ])
        self.communicator = WebsocketCommunicator(application, f"/ws/device_websocket/")
        await asyncio.sleep(1)
        connected, subprotocol = await self.communicator.connect()
        print("***Comunicator connect", self.communicator)
        await asyncio.sleep(1)
        session_id = None
        if connected:
            # Simuliram, da naprava pošlje login.
            cmd_unique_id = '1|1111111'
            await send_request(self.communicator, Commands.LOGIN, dict(device_id=DEVICE_TOKEN), cmd_unique_id, None,
                               None)
            # Tukaj moram dobiti response received in completed.
            # TODO: DC - preveri, da sta prava responsa
            response = await self.communicator_receive_from(timeout=2)
            response = await self.communicator_receive_from(timeout=2)
            response = json.loads(response)
            session_id = response.get('result').get('session_id')

        return connected, subprotocol, self.communicator, session_id

    async def communicator_receive_from(self, timeout):
        print("***Communicator receive", self.communicator)
        return await self.communicator.receive_from(timeout=timeout)

    async def close_communicator(self):
        # Close
        print('***close_communicator1')
        if self.communicator:
            print('***close_communicator2')
            await asyncio.sleep(1)
            print('***close_communicator3')
            await self.communicator.disconnect()
            await asyncio.sleep(1)
            print('***close_communicator4')
            self.communicator.stop(exceptions=False)
            print('***close_communicator5')
            self.communicator = None

    def setUp(self) -> None:
        # clear_cache()

        from django.core.cache import cache
        cache.delete("device_test_scenario")
        print(f'*** {self._testMethodName} *** Start', datetime.datetime.now())
        if self._testMethodName == 'test_error_at_command_handler':
            cache.set('device_test_scenario', 'main_handler_exception', timeout=None)
        elif self._testMethodName == 'test_error_at_command_looper':
            cache.set('device_test_scenario', 'main_looper_exception', timeout=None)
        elif self._testMethodName == 'test_error_at_command_execution':
            cache.set('device_test_scenario', 'command_execution_exception', timeout=None)
        print(f'*** Cache set ', datetime.datetime.now())

        settings.ADMINS = [('Test', 'test@scantron.si')]
        # TODO: DC - device_command_looper_run cacheja pomoje ne rabim.
        cache.delete('device_command_looper_run')
        super().setUp()

        # Namesto celery beat... za Unit teste mi ga ne uspe zagnati.
        # Zato odprem thread, ki vsako sekundo proži command handler
        self.is_running = True
        import threading
        self.beat_simulation_thread = threading.Thread(target=self.celery_beat_simulation, daemon=True)
        self.beat_simulation_thread.start()

    def tearDown(self) -> None:
        from django.core.cache import cache

        self.is_running = False
        self.beat_simulation_thread.join()
        cache.delete('device_test_scenario')
        cache.delete('device_command_looper_run')
        close_db()
        super().tearDown()
        print(f'*** {self._testMethodName} *** End', datetime.datetime.now())

    @staticmethod
    def prepare_data(device_only=False, clear_data=False):
        if clear_data:
            clear_records()
        command_id, device_id = None, None
        if device_only is not None:
            device_id = insert_device().id
        if not device_only:
            command_id = add_command().id
        return device_id, command_id

    @staticmethod
    async def prepare_data_async(device_only=False, clear_data=False):
        async_func = sync_to_async(DeviceTests.prepare_data, thread_sensitive=True)
        return await async_func(device_only, clear_data)

    async def check_with_timeout(self, chk_func, timeout):
        timeout = time.time() + timeout

        while True:
            # Moram počakati, da se komanda dejansko izvede... zato poizkušam do timeouta.
            # Če je več, potem ni v redu in test odleti.
            try:
                return await chk_func(self)
            except Exception as e:
                if time.time() < timeout:
                    await asyncio.sleep(.1)
                    continue
                else:
                    raise e

    async def check_command_sent(self, cmd, cmd_timestamp, params=None):
        if not params:
            params = {}
        self.assertTrue(cmd.cmd_sent_at is not None and cmd.cmd_sent_at < timezone.now())
        self.assertIsNone(cmd.cmd_received_at)
        self.assertGreater(cmd.cmd_sent_counter, 1)
        self.assertEqual(cmd.cmd_status, CommandStatus.SENT)
        cur_result = cmd.result_dict
        self.assertIn(cmd_timestamp, cur_result)
        cur_result = cur_result.get(cmd_timestamp, {})
        self.assertIn('device', cur_result)
        self.assertIn('sent', cur_result)
        self.assertNotIn('received', cur_result)

    async def check_command_received(self, cmd, cmd_timestamp, params=None, timeout=0):
        if not params:
            params = {}

        async def _chk_func(this):
            nonlocal cmd
            # Moram počakati, da se komanda dejansko izvede... zato poizkušam eno sekundo.
            # Če je več, potem ni v redu in test odleti.
            try:
                this.assertTrue(cmd.cmd_received_at is not None and cmd.cmd_received_at > params.get('before_received'))
                this.assertEqual(cmd.cmd_status, CommandStatus.RECEIVED)
                cur_result = cmd.result_dict
                this.assertIn(cmd_timestamp, cur_result)
                cur_result = cur_result.get(cmd_timestamp, {})
                if params.get('server_command', True):
                    this.assertIn('device', cur_result)
                    this.assertIn('sent', cur_result)
                this.assertIn('received', cur_result)
            except Exception as e:
                cmd = await get_command_object_async(cmd.unique_id(cmd_timestamp))
                raise e

        await self.check_with_timeout(_chk_func, timeout)

    async def check_command_progress(self, cmd, cmd_timestamp, params=None, timeout=0):
        if not params:
            params = {}

        async def _chk_func(this):
            nonlocal cmd
            # Moram počakati, da se komanda dejansko izvede... zato poizkušam eno sekundo.
            # Če je več, potem ni v redu in test odleti.
            try:
                this.assertEqual(cmd.cmd_status, CommandStatus.RECEIVED)
                cur_result = cmd.result_dict
                this.assertIn(cmd_timestamp, cur_result)
                cur_result = cur_result.get(cmd_timestamp, {})
                if params.get('server_command', True):
                    this.assertIn('device', cur_result)
                    this.assertIn('sent', cur_result)
                this.assertIn('received', cur_result)
                expected_progress = params.get('expected_progress', {})
                this.assertEqual(float(cmd.cmd_progress_percent), expected_progress.get('percent', -1.0))
                this.assertEqual(cmd.progress_stage_dict.get('data', {}), expected_progress.get('stage', None))
                this.assertEqual(cur_result.get('percent', {}), expected_progress.get('percent', -1.0))
                this.assertEqual(cur_result.get('stage', {}), expected_progress.get('stage', None))
                expected_result = expected_progress.get('data', None)
                if expected_result is None:
                    this.assertNotIn('result', cur_result)
                else:
                    this.assertEqual(cur_result.get('result', {}), expected_result)
            except Exception as e:
                cmd = await get_command_object_async(cmd.unique_id(cmd_timestamp))
                raise e

        await self.check_with_timeout(_chk_func, timeout)

    async def check_command_completed(self, cmd, params=None):
        if not params:
            params = {}
        status = params.get('status', CommandStatus.COMPLETED)
        self.assertIsNotNone(cmd.cmd_completed_at)
        self.assertEqual(cmd.cmd_status, status)
        cur_result = cmd.result_dict
        self.assertIn('final', cur_result)

    async def check_command_ignored(self, cmd, cmd_unique_id, ignored_reason, timeout=0):
        async def _chk_func(this):
            nonlocal cmd
            # Moram počakati, da se komanda dejansko izvede... zato poizkušam timeout sekund.
            # Če je več, potem ni v redu in test odleti.
            try:
                this.assertEqual(cmd.result_dict_timestamp(get_command_timestamp(cmd_unique_id)).get('ignored', ''),
                                 ignored_reason)
            except Exception as e:
                cmd = await get_command_object_async(cmd_unique_id)
                raise e

        await self.check_with_timeout(_chk_func, timeout)

    async def check_command_expired(self, cmd, params=None):
        if not params:
            params = {}
        self.assertEqual(cmd.cmd_status, CommandStatus.EXPIRED)

    async def check_command_cancelled(self, cmd, params=None):
        if not params:
            params = {}
        self.assertEqual(cmd.cmd_status, CommandStatus.CANCELLED)

    async def check_command_deleted(self, cmd_unique_id, device_id=None, timeout=0):
        async def _chk_func(this):
            # Moram počakati, da se komanda dejansko izvede... zato poizkušam timeout sekund.
            # Če je več, potem ni v redu in test odleti.
            command_exists = True
            try:
                await get_command_record_async(cmd_unique_id, device_id)
            except Http404:
                command_exists = False
            this.assertFalse(command_exists)

        await self.check_with_timeout(_chk_func, timeout)

    async def check_command_finished(self, cmd_unique_id, response):
        response = json.loads(response)
        self.assertEqual(response.get('status', ''), 'finished')
        self.assertEqual(response.get('response_cmd_id', ''), cmd_unique_id)
        await self.check_command_deleted(cmd_unique_id)

    async def check_log(self, cmd, status, params=None, log=None, should_exist=True):
        log = await get_log_record_async(cmd, log)
        if not should_exist:
            self.assertIsNone(log)
            return
        if not params:
            params = {}
        self.assertEqual(log.state, status)
        if params.get('cmd_null', False):
            self.assertIsNone(log.command)
        if log.state in (LogState.CMD_COMPLETED, LogState.CMD_ERROR):
            log_data = log.data_dict
            self.assertEqual(log_data.get('cmd_id', None), cmd.cmd_id)
            self.assertEqual(log_data.get('params', None), cmd.cmd_params)
            if params.get('server_command', True):
                self.assertEqual(log_data.get('executor', None), str(cmd.executor.token))
            else:
                self.assertEqual(log_data.get('device_cmd_id', None), cmd.cmd_device_cmd_id)

            self.assertEqual(log_data.get('created_at', None), json_datetime(cmd.cmd_created_at, return_format='iso'))
            self.assertEqual(log_data.get('start_at', None), json_datetime(cmd.cmd_start_at, return_format='iso'))
            self.assertEqual(log_data.get('expire_at', None), json_datetime(cmd.cmd_expire_at, return_format='iso'))
            self.assertEqual(log_data.get('sent_at', None), json_datetime(cmd.cmd_sent_at, return_format='iso'))
            self.assertEqual(log_data.get('received_at', None), json_datetime(cmd.cmd_received_at, return_format='iso'))
            # Nimam podatka, kdaj točno je bilo completed... zato, ker se je komanda zbrisala.
            # Samo preverim, če je to sploh vpisano
            if params.get('check_completed', True):
                self.assertIsNotNone(log_data.get('completed_at', None))
            self.assertEqual(log_data.get('result', None), params.get('result', ''))
            partial_resuls_cnt = params.get('partial_results', None)
            if partial_resuls_cnt is not None:
                self.assertEqual(len(log_data.get('partial_results', {})), partial_resuls_cnt)
            if params.get('check_completed', True):
                self.assertEqual(log_data.get('executing_id', None), params.get('executing_id', ''))

            if log.state == LogState.CMD_ERROR:
                self.assertEqual(log_data.get('progress_percent', None), float(cmd.cmd_progress_percent))
                self.assertEqual(log_data.get('progress_stage', None), cmd.progress_stage_dict.get('data', None))
        return log

    async def test_workflow_sender(self):
        # Testing sender command side
        # Test if sent and received statuses are properly updated in Command table
        exception = None
        try:

            # noinspection DuplicatedCode
            timeout_short, timeout_long = 2, 5
            device.api.command.COMMAND_EXECUTION_TIMEOUT_SHORT = timeout_short
            device.api.command.COMMAND_EXECUTION_TIMEOUT_LONG = timeout_long

            await self.prepare_data_async(device_only=True)

            connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            assert connected
            # noinspection PyTypeChecker
            await self.prepare_data_async(device_only=None)

            await self.communicator_receive_from(timeout=10)
            tick = timezone.now()
            # If device doesn't respond server should wait COMMAND_EXECUTION_TIMEOUT_SHORT until it sends command again.
            response = await self.communicator_receive_from(timeout=timeout_short + 2)
            self.assertGreater(timezone.now(), tick + timedelta(seconds=timeout_short))
            tick = timezone.now()

            response = json.loads(response)
            cmd_unique_id = response.get('cmd_id')

            cmd = await get_command_object_async(cmd_unique_id)
            await self.check_command_sent(cmd, get_command_timestamp(cmd_unique_id))
            cmd_log = await self.check_log(cmd, LogState.CMD_IN_PROGRESS)

            # Device responds with received.
            # Server should wait COMMAND_EXECUTION_TIMEOUT_LONG until it sends command again
            await send_response(communicator, response.get('cmd_id'), 'received', session_id=session_id)

            response = await self.communicator_receive_from(timeout=timeout_long + 2)
            self.assertGreater(timezone.now(), tick + timedelta(seconds=timeout_long))
            response = json.loads(response)
            cmd_unique_id = response.get('cmd_id')
            cmd_timestamp = get_command_timestamp(cmd_unique_id)

            before_received = timezone.now()
            await send_response(communicator, cmd_unique_id, 'received', session_id=session_id)
            # Check if received time is inserted
            cmd = await get_command_object_async(cmd_unique_id)
            await self.check_command_received(cmd, cmd_timestamp, dict(before_received=before_received), timeout=1)

            progress_data = dict(percent=0.0, stage=dict(title='Execution started'))
            await send_response(communicator, cmd_unique_id, 'progress', progress_data, session_id=session_id)
            cmd = await get_command_object_async(cmd_unique_id)
            await self.check_command_progress(cmd, cmd_timestamp, dict(expected_progress=progress_data), timeout=1)

            partial_data = ['Partial data 1', 'Partial data 2']
            progress_data = dict(percent=33.3, stage=dict(title='Stage title 1', body='Stage body 1'),
                                 data=partial_data[0])
            await send_response(communicator, cmd_unique_id, 'progress', progress_data, session_id=session_id)
            cmd = await get_command_object_async(cmd_unique_id)
            progress_data['data'] = partial_data[:1]
            await self.check_command_progress(cmd, cmd_timestamp, dict(expected_progress=progress_data), timeout=1)

            progress_data = dict(percent=66.6, stage=dict(title='Stage title 2', body='Stage body 2'),
                                 data=partial_data[1])
            await send_response(communicator, cmd_unique_id, 'progress', progress_data, session_id=session_id)
            cmd = await get_command_object_async(cmd_unique_id)
            progress_data['data'] = partial_data[:2]
            await self.check_command_progress(cmd, cmd_timestamp, dict(expected_progress=progress_data), timeout=1)

            cmd_result = 'Final data'
            await send_response(communicator, cmd_unique_id, 'completed', cmd_result, session_id=session_id)
            response = await self.communicator_receive_from(timeout=2)
            await self.check_command_finished(cmd_unique_id, response)
            await self.check_log(cmd, LogState.CMD_COMPLETED,
                                 params=dict(cmd_null=True, result=cmd_result, executing_id=cmd_timestamp), log=cmd_log)

        except Exception as e:
            exception = e

        await self.close_communicator()
        if exception:
            raise exception

    async def test_workflow_sender_multiple_command_request(self):
        # Server večkrat pošlje request za isto komando. Nazaj dobi ignored response.
        # noinspection DuplicatedCode
        exception = None
        try:
            timeout_short, timeout_long = 2, 5
            device.api.command.COMMAND_EXECUTION_TIMEOUT_SHORT = timeout_short
            device.api.command.COMMAND_EXECUTION_TIMEOUT_LONG = timeout_long

            await self.prepare_data_async(device_only=True)

            connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            assert connected

            # noinspection PyTypeChecker
            await self.prepare_data_async(device_only=None)

            # Dobim request.
            # noinspection DuplicatedCode
            response = await self.communicator_receive_from(timeout=10)
            response = json.loads(response)
            cmd_unique_id = response.get('cmd_id')
            cmd_unique_id_running = cmd_unique_id + '0'
            cmd = await get_command_object_async(cmd_unique_id_running)
            cmd_log = await self.check_log(cmd, LogState.CMD_IN_PROGRESS)

            # Odgovorim z received
            await send_response(communicator, cmd_unique_id, 'received', session_id=session_id)
            # Potem ignored
            ignored_reason = "Command already executing"
            tick = timezone.now()
            await send_response(communicator, cmd_unique_id, 'ignored', ignored_reason, session_id=session_id)
            await asyncio.sleep(.2)
            # Preverim, če se je ignored reason zapisal v command result
            cmd = await get_command_object_async(cmd_unique_id_running)
            self.assertEqual(cmd.result_dict_timestamp(get_command_timestamp(cmd_unique_id)).get('ignored', ''),
                             ignored_reason)
            # Počakam, da vidim, če se pošiljanje komande dejansko zaklene za COMMAND_EXECUTION_TIMEOUT_SHORT
            # Dobim request
            response = await self.communicator_receive_from(timeout=timeout_short + 2)
            self.assertGreater(timezone.now(), tick + timedelta(seconds=timeout_short))
            response = json.loads(response)
            cmd_unique_id = response.get('cmd_id')
            # Odgovorim z received
            await send_response(communicator, cmd_unique_id, 'received', session_id=session_id)
            # Potem ignored
            await send_response(communicator, cmd_unique_id, 'ignored', ignored_reason, session_id=session_id)
            # Pošljem trenutni progress
            progress_data = dict(percent=0.0, stage=dict(title='Execution started'), data=[], all_data=True)
            tick = timezone.now()
            await send_response(communicator, cmd_unique_id_running, 'progress', progress_data, session_id=session_id)
            # Počakam, da vidim, če se pošiljanje komande dejansko zaklene za COMMAND_EXECUTION_TIMEOUT_LONG
            # Dobim request
            await asyncio.sleep(.2)
            # Preverim, če se je ignored reason zapisal v command result
            cmd = await get_command_object_async(cmd_unique_id_running)
            self.assertEqual(cmd.result_dict_timestamp(get_command_timestamp(cmd_unique_id)).get('ignored', ''),
                             ignored_reason)
            response = await self.communicator_receive_from(timeout=timeout_long + 2)
            self.assertGreater(timezone.now(), tick + timedelta(seconds=timeout_long))
            response = json.loads(response)
            cmd_unique_id = response.get('cmd_id')
            # Odgovorim z received
            await send_response(communicator, cmd_unique_id, 'received', session_id=session_id)
            # Potem ignored
            await send_response(communicator, cmd_unique_id, 'ignored', ignored_reason, session_id=session_id)
            # Pošljem trenutni progress
            progress_data = dict(percent=0.0, stage=dict(title='Execution started'), data=[], all_data=True)
            tick = timezone.now()
            await send_response(communicator, cmd_unique_id_running, 'progress', progress_data, session_id=session_id)
            await asyncio.sleep(.5)
            # Preverim, če se je ignored reason zapisal v command result
            cmd = await get_command_object_async(cmd_unique_id_running)
            self.assertEqual(cmd.result_dict_timestamp(get_command_timestamp(cmd_unique_id)).get('ignored', ''),
                             ignored_reason)

            # Pošljem completed
            cmd_result = 'Final data'
            cmd = await get_command_object_async(cmd_unique_id_running)
            await send_response(communicator, cmd_unique_id_running, 'completed', cmd_result, session_id=session_id)
            response = await self.communicator_receive_from(timeout=2)
            # Preverim, če je v logu zapisano vse kar mora biti
            #  - Mora imeti tudi 4 delne rezultate: 3x ignored in 1 x progress za pravi request.
            await self.check_command_finished(cmd_unique_id_running, response)
            await self.check_log(cmd, LogState.CMD_COMPLETED,
                                 params=dict(cmd_null=True, result=cmd_result, partial_results=4,
                                             executing_id=get_command_timestamp(cmd_unique_id_running)), log=cmd_log)

            # Dodam še eno komando, da lahko stestiram, kaj se zgodi, če je komanda na napravi zaključena...
            # in samo vrne rezultat izvajanja
            # noinspection PyTypeChecker
            await self.prepare_data_async(None)
            # Dobim request
            response = await self.communicator_receive_from(timeout=10)
            # noinspection DuplicatedCode
            response = json.loads(response)
            cmd_unique_id = response.get('cmd_id')
            cmd_unique_id_completed = cmd_unique_id + '0'
            cmd = await get_command_object_async(cmd_unique_id_completed)
            cmd_log = await self.check_log(cmd, LogState.CMD_IN_PROGRESS)

            # Odgovorim z received
            await send_response(communicator, cmd_unique_id, 'received', session_id=session_id)

            # Odgovorim z completed/error
            cmd_result = 'Execution error'
            ignored_reason = "Command already completed"

            await send_response(communicator, cmd_unique_id, 'ignored', ignored_reason, session_id=session_id)
            cmd = await get_command_object_async(cmd_unique_id_completed)

            await self.check_command_ignored(cmd, cmd_unique_id, ignored_reason, timeout=1)
            cmd = await get_command_object_async(cmd_unique_id_completed)

            await send_response(communicator, cmd_unique_id_completed, 'error', cmd_result, session_id=session_id)
            response = await self.communicator_receive_from(timeout=2)

            # Preverim, če je v logu zapisano vse kar mora biti
            #  - Mora imeti tudi 2 delna rezultata: 1x ignored in 1 x response za pravi request.
            await self.check_command_finished(cmd_unique_id_completed, response)
            await self.check_log(cmd, LogState.CMD_ERROR,
                                 params=dict(cmd_null=True, result=cmd_result, partial_results=2,
                                             executing_id=get_command_timestamp(cmd_unique_id_completed)), log=cmd_log)
        except Exception as e:
            exception = e

        await self.close_communicator()
        if exception:
            raise exception

    async def test_workflow_sender_cancel(self):
        # Server pošlje na napravo komando za izvajanje, potem jo pa cancela
        exception = None
        try:
            await self.prepare_data_async(True)

            connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            assert connected

            # noinspection PyTypeChecker
            await self.prepare_data_async(None)

            # Prileti komanda, za katero se moram pretvarjati da dolgo teče.
            response = await self.communicator_receive_from(timeout=10)
            cmd_unique_id = json.loads(response).get('cmd_id')
            cmd_timestamp = get_command_timestamp(cmd_unique_id)
            cmd = await get_command_object_async(cmd_unique_id)
            cmd_log = await self.check_log(cmd, LogState.CMD_IN_PROGRESS)
            await send_response(communicator, cmd_unique_id, 'received', session_id=session_id)
            progress_data = dict(percent=0.0, stage=dict(title='Execution started'))
            await send_response(communicator, cmd_unique_id, 'progress', progress_data, session_id=session_id)

            # Na serverju generiram še cancel komando.
            # noinspection DuplicatedCode
            await add_command_async(Commands.CANCEL_DEVICE_COMMAND, dict(cancel_cmd_id=cmd.cmd_id))

            # Na cancel komando moram odgovoriti, kot na vsako drugo.
            response = await self.communicator_receive_from(timeout=10)
            cmd_cancel_unique_id = json.loads(response).get('cmd_id')
            cmd_cancel_timestamp = get_command_timestamp(cmd_cancel_unique_id)
            cmd_cancel = await get_command_object_async(cmd_cancel_unique_id)
            # Log za cancel comando ne sme obstajati... ker je tako zapisano v definiciji komande
            await self.check_log(cmd_cancel, None, should_exist=False)
            await send_response(communicator, cmd_cancel_unique_id, 'received', session_id=session_id)
            progress_data = dict(percent=0.0, stage=dict(title='Execution started'))
            await send_response(communicator, cmd_cancel_unique_id, 'progress', progress_data, session_id=session_id)
            # Potem ko pošljem. da je cancel comanda completed. Pošljem še, da je originalna komanda canceled.
            await send_response(communicator, cmd_cancel_unique_id, 'completed', 'Command terminated',
                                session_id=session_id)

            # Dobim nazaj finished tako za cancelano komando in tudi cancel komando
            response = await self.communicator_receive_from(timeout=10)
            await self.check_command_finished(cmd_cancel_unique_id, response)
            cmd = await get_command_object_async(cmd_unique_id)
            await send_response(communicator, cmd_unique_id, 'cancelled', 'Command cancelled', session_id=session_id)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_command_finished(cmd_unique_id, response)

            # Preverim log za cancellano komando
            cmd_result = 'Command cancelled'
            await self.check_log(cmd, LogState.CMD_ERROR,
                                 params=dict(cmd_null=True, result=cmd_result, executing_id=cmd_timestamp), log=cmd_log)

            # Potem pa na serverju generiram še dve cancel komandi...
            # ena hoče cancelati zaključeno komando
            # noinspection DuplicatedCode
            await add_command_async(Commands.CANCEL_DEVICE_COMMAND, dict(cancel_cmd_id=cmd.cmd_id))
            response = await self.communicator_receive_from(timeout=10)
            cmd_cancel_unique_id = json.loads(response).get('cmd_id')
            cmd_cancel_timestamp = get_command_timestamp(cmd_cancel_unique_id)
            cmd_cancel = await get_command_object_async(cmd_cancel_unique_id)
            # Log za cancel comando ne sme obstajati... ker je tako zapisano v definiciji komande
            await self.check_log(cmd_cancel, None, should_exist=False)
            await send_response(communicator, cmd_cancel_unique_id, 'received', session_id=session_id)
            progress_data = dict(percent=0.0, stage=dict(title='Execution started'))
            await send_response(communicator, cmd_cancel_unique_id, 'progress', progress_data, session_id=session_id)

            cmd_cancel = await get_command_object_async(cmd_cancel_unique_id)
            # Tukaj počakam, da se v komando vpiše progress data
            tick = time.time()
            while time.time() - tick < 2:
                if cmd_cancel.cmd_progress_stage_data:
                    break
                cmd_cancel = await get_command_object_async(cmd_cancel_unique_id)

            error_data = dict(type='Process error', text='No worker for requested command found!')
            # Potem ko pošljem. da je cancel comanda error.
            await send_response(communicator, cmd_cancel_unique_id, 'error', error_data, session_id=session_id)
            # Dobim nazaj finished tako za cancelano komando in tudi cancel komando
            response = await self.communicator_receive_from(timeout=10)
            await self.check_command_finished(cmd_cancel_unique_id, response)
            # Sedaj bi moral log obstajati... samo težko ga najdem, ker ima command field = null
            # Najdem zadnjega v bazi. To dosežem, če nastavim log = False
            await self.check_log(cmd_cancel, LogState.CMD_ERROR,
                                 params=dict(cmd_null=True, result=error_data, executing_id=cmd_cancel_timestamp),
                                 log=False)

            # druga pa neobstoječo
            await add_command_async(Commands.CANCEL_DEVICE_COMMAND, dict(cancel_cmd_id=99))
            # noinspection DuplicatedCode
            response = await self.communicator_receive_from(timeout=10)
            cmd_cancel_unique_id = json.loads(response).get('cmd_id')
            cmd_cancel_timestamp = get_command_timestamp(cmd_cancel_unique_id)
            cmd_cancel = await get_command_object_async(cmd_cancel_unique_id)
            # Log za cancel comando ne sme obstajati... ker je tako zapisano v definiciji komande
            await self.check_log(cmd_cancel, None, should_exist=False)
            await send_response(communicator, cmd_cancel_unique_id, 'received', session_id=session_id)
            progress_data = dict(percent=0.0, stage=dict(title='Execution started'))
            await send_response(communicator, cmd_cancel_unique_id, 'progress', progress_data, session_id=session_id)

            cmd_cancel = await get_command_object_async(cmd_cancel_unique_id)
            # Tukaj počakam, da se v komando vpiše progress data
            tick = time.time()
            while time.time() - tick < 2:
                if cmd_cancel.cmd_progress_stage_data:
                    break
                cmd_cancel = await get_command_object_async(cmd_cancel_unique_id)

            error_data = dict(type='Process error', text='No Command matches the given query.!')
            # Potem ko pošljem. da je cancel comanda error.
            await send_response(communicator, cmd_cancel_unique_id, 'error', error_data, session_id=session_id)
            # Dobim nazaj finished tako za cancelano komando in tudi cancel komando
            response = await self.communicator_receive_from(timeout=10)
            await self.check_command_finished(cmd_cancel_unique_id, response)
            # Sedaj bi moral log obstajati... samo težko ga najdem, ker ima command field = null
            # Najdem zadnjega v bazi. To dosežem, če nastavim log = False
            await self.check_log(cmd_cancel, LogState.CMD_ERROR,
                                 params=dict(cmd_null=True, result=error_data, executing_id=cmd_cancel_timestamp),
                                 log=False)
        except Exception as e:
            exception = e

        await self.close_communicator()
        if exception:
            raise exception

    async def test_workflow_sender_bad_request(self):
        # Server pošlje na napravo napačen request
        exception = None
        try:
            await self.prepare_data_async()

            connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            assert connected

            # Prileti komanda, na katero odgovorim, kot, da je invalid.
            response = await self.communicator_receive_from(timeout=10)
            cmd_unique_id = json.loads(response).get('cmd_id')
            cmd_timestamp = get_command_timestamp(cmd_unique_id)
            cmd = await get_command_object_async(cmd_unique_id)
            cmd_log = await self.check_log(cmd, LogState.CMD_IN_PROGRESS)

            result = dict(type='Invalid request', text='Unknown command id -20')
            await send_response(communicator, cmd_unique_id, 'invalid', data=result, params=response,
                                session_id=session_id)
            await self.check_command_deleted(cmd_unique_id, timeout=1)
            await self.check_log(cmd, LogState.CMD_ERROR,
                                 params=dict(cmd_null=True, result=result, executing_id=cmd_timestamp), log=cmd_log)

            if await communicator.receive_nothing(3, 0.5):
                response = None
            else:
                response = await self.communicator_receive_from(timeout=3)
            self.assertIsNone(response)  # Od serverja ne sme nič več priti

            # Izgleda se komunikator zacikla... ponovno vzpostavim povezavo
            # await self.close_communicator()
            # connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            # assert connected

            # Pošljem invalid response za komando, ki jo server ne spozna. Mora se poslati mail adminu.
            with self.settings(EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend'):
                result = dict(type='Invalid request', text='Unknown command id -20')
                await send_response(communicator, 'lalala', 'invalid', data=result, params=response,
                                    session_id=session_id)

                for i in range(50):
                    if len(mail.outbox) > 0:
                        break
                    await asyncio.sleep(.1)

                self.assertTrue(len(mail.outbox) > 0)
                error_mail = mail.outbox[0]
                self.assertEqual(error_mail.to, list(admin_mail[1] for admin_mail in settings.ADMINS))
                self.assertIn('Invalid command request', error_mail.subject)

        except Exception as e:
            exception = e

        await self.close_communicator()
        if exception:
            raise exception

    async def check_response(self, response, status, cmd_unique_id, params=None):
        if not params:
            params = {}
        response = json.loads(response)
        self.assertEqual(response.get('status', ''), status)
        self.assertEqual(response.get('response_cmd_id', ''), cmd_unique_id)
        if status == 'progress':
            result = response.get('result', {})
            if 'expected_progress' in params:
                self.assertEqual(params.get('expected_progress'), result)
            else:
                self.assertIn('percent', result)
                self.assertIn('stage', result)
                self.assertIn('data', result)
        elif status in ('completed', 'error'):
            self.assertEqual(response.get('result', None), params.get('result', {}))
        elif status == 'ignored':
            self.assertEqual(response.get('result', None), params.get('result', ''))
        elif status == 'invalid':
            self.assertEqual(response.get('result', {}).get('type', None), "Invalid request")
            self.assertEqual(response.get('result', {}).get('text', None), params.get('expected_text'))
            request_params = json.loads(response.get('params', {}))
            request_params.pop('_sent_timestamp', None)
            self.assertEqual(request_params, params.get('expected_params'))

    async def test_workflow_receiver(self):
        # Naprava pošlje komando na server
        exception = None
        try:
            device_id, command_id = await self.prepare_data_async(True)

            connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            assert connected

            for cmd_id in ('10', '11'):
                # cmd_id 10 should receive completed status
                # cmd_id 11 should receive error status
                raise_error = cmd_id == '11'
                cmd_unique_id = '%s|123456' % cmd_id
                cmd_timestamp = get_command_timestamp(cmd_unique_id)
                before_received = timezone.now()
                await asyncio.sleep(.001)
                await send_request(communicator, Commands.TEST_INCOMING_LOG, dict(raise_error=raise_error),
                                   cmd_unique_id, None, session_id)
                response = await self.communicator_receive_from(timeout=10)
                # Tukaj moram dobiti response received.
                await self.check_response(response, 'received', cmd_unique_id)
                cmd = await get_command_object_async(cmd_unique_id, device_id)
                await self.check_command_received(cmd, cmd_timestamp,
                                                  dict(before_received=before_received, server_command=False))

                response = await self.communicator_receive_from(timeout=10)
                # Potem dobim response progress
                await self.check_response(response, 'progress', cmd_unique_id)
                cmd = await get_command_object_async(cmd_unique_id, device_id)
                await self.check_log(cmd, LogState.CMD_IN_PROGRESS)
                progress_data = dict(percent=0.0, stage=dict(title='Execution started'))
                await self.check_command_progress(cmd, cmd_timestamp,
                                                  dict(server_command=False, expected_progress=progress_data))

                response = await self.communicator_receive_from(timeout=10)
                progress_data = dict(percent=33.3, stage=dict(title='Stage title 1', body='Stage body 1'),
                                     data='Partial data 1', all_data=False)

                await self.check_response(response, 'progress', cmd_unique_id, dict(expected_progress=progress_data))
                cmd = await get_command_object_async(cmd_unique_id, device_id)
                progress_data['data'] = ['Partial data 1']
                await self.check_command_progress(cmd, cmd_timestamp,
                                                  dict(server_command=False, expected_progress=progress_data))

                response = await self.communicator_receive_from(timeout=10)
                progress_data = dict(percent=66.6, stage=dict(title='Stage title 2', body='Stage body 2'),
                                     data='Partial data 2', all_data=False)

                await self.check_response(response, 'progress', cmd_unique_id, dict(expected_progress=progress_data))
                cmd = await get_command_object_async(cmd_unique_id, device_id)
                progress_data['data'] = ['Partial data 1', 'Partial data 2']
                await self.check_command_progress(cmd, cmd_timestamp,
                                                  dict(server_command=False, expected_progress=progress_data))

                if raise_error:
                    cmd_result = dict(type='Process error', text='command_test_incoming_log exeption')
                    response_status = 'error'
                    log_state = LogState.CMD_ERROR
                    cmd_status = CommandStatus.ERROR
                else:
                    cmd_result = 'command_test_incoming_log executed'
                    response_status = 'completed'
                    log_state = LogState.CMD_COMPLETED
                    cmd_status = CommandStatus.COMPLETED

                response = await self.communicator_receive_from(timeout=10)
                await self.check_response(response, response_status, cmd_unique_id, dict(result=cmd_result))
                cmd = await get_command_object_async(cmd_unique_id, device_id)
                await self.check_command_completed(cmd, dict(status=cmd_status))
                await self.check_log(cmd, log_state,
                                     params=dict(server_command=False, partial_results=1, result=cmd_result,
                                                 executing_id=cmd_timestamp))
                await send_response(communicator, cmd_unique_id, 'finished', session_id=session_id)
                await self.check_command_deleted(cmd_unique_id, device_id, timeout=1)
        except Exception as e:
            exception = e

        await self.close_communicator()
        if exception:
            raise exception

    async def test_workflow_receiver_bad_request(self):
        # Naprava pošlje na server napačen request
        exception = None
        try:

            await self.prepare_data_async(True)

            connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            assert connected

            cmd_unique_id = '10|123456'
            await asyncio.sleep(.001)
            # Request z napačnim command type-om
            invalid_cmd = 20
            await send_request(communicator, invalid_cmd, None, cmd_unique_id, None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'invalid', cmd_unique_id, dict(
                expected_text=f"Unknown command id {invalid_cmd}",
                expected_params=dict(cmd=invalid_cmd, params=None, cmd_id=cmd_unique_id, expire_at=json_datetime(None),
                                     _session_id=session_id)))

            # Request z brez command type-a
            await send_request(communicator, None, None, cmd_unique_id, None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'invalid', cmd_unique_id, dict(
                expected_text="Request without 'response_cmd_id' parameter must have 'cmd' parameter.",
                expected_params=dict(params=None, cmd_id=cmd_unique_id, expire_at=json_datetime(None),
                                     _session_id=session_id)))

            # Request z None command unique ID-jem
            await send_request(communicator, Commands.TEST_INCOMING_LOG, None, None, None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'invalid', '__none__', dict(
                expected_text="Request without 'response_cmd_id' parameter must have 'cmd_id' parameter.",
                expected_params=dict(cmd=Commands.TEST_INCOMING_LOG, params=None, cmd_id=None,
                                     expire_at=json_datetime(None), _session_id=session_id)))

            # Request z praznim command unique ID-ja
            await send_request(communicator, Commands.TEST_INCOMING_LOG, None, '', None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'invalid', '', dict(
                expected_text="Request without 'response_cmd_id' parameter must have 'cmd_id' parameter.",
                expected_params=dict(cmd=Commands.TEST_INCOMING_LOG, params=None, cmd_id='',
                                     expire_at=json_datetime(None), _session_id=session_id)))

            # Request z command unique ID-jem v napačnem formatu
            await send_request(communicator, Commands.TEST_INCOMING_LOG, None, 'asdfasfd', None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'invalid', 'asdfasfd', dict(
                expected_text="Parameter 'cmd_id' must be in '{cmd_id}|{timestamp epoch}' format.",
                expected_params=dict(cmd=Commands.TEST_INCOMING_LOG, params=None, cmd_id='asdfasfd',
                                     expire_at=json_datetime(None), _session_id=session_id)))

            # Request z napačnim session ID-jem
            await send_request(communicator, Commands.TEST_INCOMING_LOG, None, '12|123456', None, 'abcd')
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'invalid', '12|123456', dict(
                expected_text="Wrong session id. Please login to server again",
                expected_params=dict(cmd=Commands.TEST_INCOMING_LOG, params=None, cmd_id='12|123456',
                                     expire_at=json_datetime(None), _session_id='abcd')))

            # Request za neregistriran device
            await update_device_record_async(DEVICE_TOKEN, dict(state=DeviceState.UNREGISTERED))

            await send_request(communicator, Commands.TEST_INCOMING_LOG, None, '12|123456', None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'invalid', '12|123456', dict(
                expected_text="Device is currently not allowed to communicate with server",
                expected_params=dict(cmd=Commands.TEST_INCOMING_LOG, params=None, cmd_id='12|123456',
                                     expire_at=json_datetime(None), _session_id=session_id)))
        except Exception as e:
            exception = e

        await self.close_communicator()
        if exception:
            raise exception

    async def test_error_at_command_handler(self):
        # Error je lahko izven main loopa... takrat se tudi main loop neha izvajati
        with self.settings(EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend'):
            mail.outbox.clear()
            for i in range(50):
                if len(mail.outbox) > 0:
                    break
                await asyncio.sleep(.1)

            self.assertTrue(len(mail.outbox) == 1)
            error_mail = mail.outbox[0]
            self.assertEqual(error_mail.to, list(admin_mail[1] for admin_mail in settings.ADMINS))
            self.assertIn('Exception in main command handler', error_mail.subject)
            self.assertIn('Main handler exception', error_mail.body)

    async def test_error_at_command_looper(self):
        # Error je lahko znotraj main loopa... je pohandlan... main loop se nadaljuje
        with self.settings(EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend'):
            mail.outbox.clear()
            for i in range(50):
                if len(mail.outbox) > 1:  # Počakam, da sta vsaj 2 maila
                    break
                await asyncio.sleep(.1)

            self.assertTrue(len(mail.outbox) > 1)
            for mail_cnt in range(2):
                error_mail = mail.outbox[mail_cnt]
                self.assertEqual(error_mail.to, list(admin_mail[1] for admin_mail in settings.ADMINS))
                self.assertIn('Exception in command handler looper', error_mail.subject)
                self.assertIn('Main looper exception', error_mail.body)

    async def test_error_at_command_execution(self):
        # Error je lahko pri dejanskem izvajanju komande (recimo executor == null).
        #   Komanda se ponovno začne izvajati naslednji loop
        with self.settings(EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend'):
            mail.outbox.clear()
            device_id, command_id = await self.prepare_data_async()
            for i in range(50):
                if len(mail.outbox) > 1:  # Počakam, da sta vsaj 2 maila
                    break
                await asyncio.sleep(.1)
            self.assertTrue(len(mail.outbox) > 1)
            for mail_cnt in range(2):
                error_mail = mail.outbox[mail_cnt]
                self.assertEqual(error_mail.to, list(admin_mail[1] for admin_mail in settings.ADMINS))
                self.assertIn('Exception during handling command: %d' % command_id, error_mail.subject)
                self.assertIn('Command execution exception', error_mail.body)

    async def test_workflow_receiver_multiple_command_request(self):
        # Server večkrat dobi request za isto komando.
        exception = None
        try:
            device_id, command_id = await self.prepare_data_async(True)

            connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            assert connected

            cmd_unique_id_first = '10|123456'
            await asyncio.sleep(.001)
            await send_request(communicator, Commands.TEST_INCOMING_LOG, None, cmd_unique_id_first, None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            # Tukaj moram dobiti response received.
            await self.check_response(response, 'received', cmd_unique_id_first)
            # Tukaj se komanda na serverju izvaja. Ni pa še zaključena.
            # TODO: DC - ali bi lahko namesto sleep-a naredil dejansko preverjanje (za vse sleepe)
            await asyncio.sleep(2)
            # Ponovno pošljem request na server
            cmd_unique_id_second = '10|234567'
            await send_request(communicator, Commands.TEST_INCOMING_LOG, None, cmd_unique_id_second, None, session_id)
            # Počakam, da dobim najprej 'received' response. Potem pa še ignored.
            response_dict = {}
            while not (response_dict.get('status', '') == 'received' and
                       response_dict.get('response_cmd_id', '') == cmd_unique_id_second):
                response = await self.communicator_receive_from(timeout=10)
                response_dict = json.loads(response)
            await self.check_response(response, 'received', cmd_unique_id_second)

            while not (response_dict.get('status', '') == 'ignored' and
                       response_dict.get('response_cmd_id', '') == cmd_unique_id_second):
                response = await self.communicator_receive_from(timeout=10)
                response_dict = json.loads(response)
            await self.check_response(response, 'ignored', cmd_unique_id_second,
                                      dict(result='Command already executing'))

            cmd = await get_command_object_async(cmd_unique_id_first, device_id)
            # Preverim, če se je bil drugi request za komando zaznan.
            #  - Povečan sent_counter
            #  - V result key-ju za drugi timestamp je zabeležen received
            self.assertEqual(cmd.cmd_sent_counter, 2)
            self.assertIn('received', cmd.result_dict_timestamp(get_command_timestamp(cmd_unique_id_second)))

            # Potem moram pa dobiti še cel progress za komando, ki se trenutno izvaja.
            while not (response_dict.get('status', '') == 'progress' and
                       response_dict.get('response_cmd_id', '') == cmd_unique_id_first and
                       response_dict.get('result', {}).get('all_data', False)):
                response = await self.communicator_receive_from(timeout=10)
                response_dict = json.loads(response)
            progress_data = dict(percent=float(cmd.cmd_progress_percent), stage=cmd.progress_stage_dict.get('data', {}),
                                 data=cmd.partial_result(cmd_unique_id_first), all_data=True)

            await self.check_response(response, 'progress', cmd_unique_id_first, dict(expected_progress=progress_data))

            # Počakam, da se komanda izvede do konca
            while not (response_dict.get('status', '') == 'completed' and
                       response_dict.get('response_cmd_id', '') == cmd_unique_id_first):
                response = await self.communicator_receive_from(timeout=10)
                response_dict = json.loads(response)

            # Ker nisem poslal finished se podatki za komando ne zbrišejo.
            # Če ponovno pošljem zahtevo za isto komando bi moral takoj dobiti nazaj rezulat.
            cmd_unique_id_third = '10|345678'
            cmd_result = "command_test_incoming_log executed"
            await send_request(communicator, Commands.TEST_INCOMING_LOG, None, cmd_unique_id_third, None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'received', cmd_unique_id_third)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'ignored', cmd_unique_id_third,
                                      dict(result='Command already completed'))
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'completed', cmd_unique_id_first, dict(result=cmd_result))
            cmd = await get_command_object_async(cmd_unique_id_third, device_id)
            # Preverim, če se je bil tretji request za komando zaznan.
            #  - Povečan sent_counter
            #  - V result key-ju za drugi timestamp je zabeležen received
            self.assertEqual(cmd.cmd_sent_counter, 3)
            self.assertIn('received', cmd.result_dict_timestamp(get_command_timestamp(cmd_unique_id_third)))

            # Serverju pošljem dva requesta za isto komando zapored. Nazaj bi moral priti odgovor samo za prvega.
            # zadnjega bi pa moral ignorirati.
            log_count = await get_log_record_count_async()

            cmd_result = "command_test_incoming_nolog executed"
            cmd_unique_id_first = '11|123456'
            cmd_unique_id_second = '11|234567'
            await send_request(communicator, Commands.TEST_INCOMING_NO_LOG, None, cmd_unique_id_first, None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            # Tukaj moram dobiti response received.
            await self.check_response(response, 'received', cmd_unique_id_first)

            response = await self.communicator_receive_from(timeout=10)
            # Potem dobim response progress
            await self.check_response(response, 'progress', cmd_unique_id_first)

            await send_request(communicator, Commands.TEST_INCOMING_NO_LOG, None, cmd_unique_id_second, None,
                               session_id)
            response = await self.communicator_receive_from(timeout=10)
            # Tukaj moram dobiti response received.
            await self.check_response(response, 'received', cmd_unique_id_second)

            # Lahko dobim ignored za drugo komando in full progress za prvo komando... to preskočim
            response = await self.communicator_receive_from(timeout=10)
            response_dict = json.loads(response)
            while (
                (
                    response_dict.get('status', '') == 'ignored' and
                    response_dict.get('response_cmd_id', '') == cmd_unique_id_second
                ) or
                (
                    response_dict.get('status', '') == 'progress' and
                    response_dict.get('response_cmd_id', '') == cmd_unique_id_first and
                    response_dict.get('result', {}).get('all_data', False)
                )
            ):
                response = await self.communicator_receive_from(timeout=10)
                response_dict = json.loads(response)

            # Pa še completed za prvo komando

            await self.check_response(response, 'completed', cmd_unique_id_first, dict(result=cmd_result))

            await send_response(communicator, cmd_unique_id_first, 'finished', session_id=session_id)
            await self.check_command_deleted(cmd_unique_id_first, device_id, timeout=1)
            # Preverim, da se log ni zapisal (Komanda je označena, kot da se zanjo ne piše log)
            self.assertEqual(log_count, await get_log_record_count_async())
        except Exception as e:
            exception = e

        await self.close_communicator()
        if exception:
            raise exception

    async def test_workflow_receiver_expired(self):
        # Server večkrat dobi request za isto komando.
        exception = None
        try:
            device_id, command_id = await self.prepare_data_async(True)

            connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            assert connected

            cmd_unique_id_first = '10|123456'
            await asyncio.sleep(.001)
            await send_request(communicator, Commands.TEST_INCOMING_LOG, None, cmd_unique_id_first,
                               timezone.now() - timedelta(hours=1), session_id)
            response = await self.communicator_receive_from(timeout=10)
            # Tukaj moram dobiti response received.
            await self.check_response(response, 'received', cmd_unique_id_first)

            # Potem pa še ignored/expired
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'ignored', cmd_unique_id_first, dict(result='Command expired'))
            # Tukaj mora komanda še obstajati.
            cmd = await get_command_object_async(cmd_unique_id_first, device_id)
            # Preverim, če je komanda expired.
            await self.check_command_expired(cmd)
            # Preverim, če obtaja error log.
            await self.check_log(cmd, LogState.CMD_ERROR,
                                 params=dict(server_command=False, cmd_null=False, result='Command expired',
                                             check_completed=False, executing_id=cmd_unique_id_first))

            # Spremenim, da je bila komanda kreirana že več kot en dan nazaj
            await update_command_record_async(cmd_unique_id_first, device_id,
                                              dict(created_at=timezone.now() - timedelta(days=1)))

            # Preverim, da komande ni več
            await self.check_command_deleted(cmd_unique_id_first, device_id, timeout=2)
        except Exception as e:
            exception = e

        await self.close_communicator()
        if exception:
            raise exception

    async def test_workflow_receiver_cancel(self):
        # Server dobi zahtevo za cancel komande
        exception = None
        try:
            device_id, command_id = await self.prepare_data_async(True)

            connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            assert connected
            print('***0.1')

            cmd_result = 'Command terminated'
            cmd_unique_id_first = '10|123456'
            request_progress_data = []
            await asyncio.sleep(.001)
            await send_request(communicator, Commands.TEST_INCOMING_LOG, None, cmd_unique_id_first, None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            # Tukaj moram dobiti response received.
            await self.check_response(response, 'received', cmd_unique_id_first)
            await asyncio.sleep(1)
            cmd_unique_id_second = '11|123457'
            # Potem pa pošljem komando za cancel komande
            print('***0.2')
            await send_request(communicator, Commands.CANCEL_SERVER_COMMAND, dict(cancel_cmd_id=10),
                               cmd_unique_id_second, None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            # Počakam, da pride received event za cancel komando.
            # Vmes lahko prihajajo progress eventi za originalno komando.
            response_dict = json.loads(response)
            while not (response_dict.get('status', '') == 'received' and
                       response_dict.get('response_cmd_id', '') == cmd_unique_id_second):
                print('***0.3')
                if (
                    response_dict.get('status', '') == 'progress' and
                    response_dict.get('response_cmd_id', '') == cmd_unique_id_first
                ):
                    request_progress_data.append(response_dict)
                response = await self.communicator_receive_from(timeout=10)
                response_dict = json.loads(response)

            print('***0.4')
            await self.check_response(response, 'received', cmd_unique_id_second)
            await asyncio.sleep(2)
            # Potem moram pa dobiti dva responsa...
            # za cmd_unique_id_second, da je completed (In nima loga)
            # Vmes lahko hodijo progress statusi
            response = await self.communicator_receive_from(timeout=10)
            response_dict = json.loads(response)
            while not (response_dict.get('status', '') == 'completed' and
                       response_dict.get('response_cmd_id', '') == cmd_unique_id_second):
                print('***0.5')
                if (
                    response_dict.get('status', '') == 'progress' and
                    response_dict.get('response_cmd_id', '') == cmd_unique_id_first
                ):
                    request_progress_data.append(response_dict)
                response = await self.communicator_receive_from(timeout=10)
                response_dict = json.loads(response)

            print('***0.6')
            await self.check_response(response, 'completed', cmd_unique_id_second, dict(result=cmd_result))

            print('***1')
            # ... za cmd_unique_id_first, da je cancelled
            # (In ima log... ker je tako določeno v parametrih komande)
            # Vmes lahko hodijo progress statusi
            response = await self.communicator_receive_from(timeout=10)
            response_dict = json.loads(response)
            while not (response_dict.get('status', '') == 'cancelled' and
                       response_dict.get('response_cmd_id', '') == cmd_unique_id_first):
                if (
                    response_dict.get('status', '') in ('progress', 'completed') and
                    response_dict.get('response_cmd_id', '') == cmd_unique_id_first
                ):
                    request_progress_data.append(response_dict)
                response = await self.communicator_receive_from(timeout=10)
                response_dict = json.loads(response)

            print('***2')
            await self.check_response(response, 'cancelled', cmd_unique_id_first)
            # Moram narediti še preverjanje, da niso prišli vsi progress statusi...
            # se pravi, da se je task dejansko ubil.
            try:
                # Počakam, če bo mogoče prišel še kakšen progress status.
                while True:
                    if await communicator.receive_nothing(10, 0.5):
                        break
                    print('***3')
                    response = await self.communicator_receive_from(timeout=10)
                    response_dict = json.loads(response)
                    if (
                        response_dict.get('status', '') in ('progress', 'completed') and
                        response_dict.get('response_cmd_id', '') == cmd_unique_id_first
                    ):
                        request_progress_data.append(response_dict)
            except:
                pass
            print('***4')
            if len(request_progress_data) and request_progress_data[-1].get('status') == 'completed':
                self.fail("Command was not cancelled")
            print('***5')

            # Preverim še log.
            # Preverim, da se cancelled komanda briše po enem dnevu.
            # Tukaj mora komanda še obstajati.
            cmd = await get_command_object_async(cmd_unique_id_first, device_id)
            # Preverim, če je komanda cancelled.
            await self.check_command_cancelled(cmd)
            # Preverim, če obtaja error log.
            await self.check_log(cmd, LogState.CMD_ERROR,
                                 params=dict(server_command=False, cmd_null=False, result='Command cancelled',
                                             check_completed=False, executing_id=cmd_unique_id_first))

            # Ker je imel komunkator nazandnje timeout error, ga moram šeenkrat zagnati
            print('***6')
            # await self.close_communicator()
            # connected, subprotocol, communicator, session_id = await self.connect_to_ws()
            print('***8')
            # assert connected
            print('***9')

            # Še enkrat probam cancelati task... tokrat mora vrniti napako.
            cmd_result = dict(type='Process error', text='No worker for requested command found!')
            cmd_unique_id_third = '12|123458'
            # Potem pa pošljem komando za cancel komande, ki ne obstaja
            print('***10')
            await send_request(communicator, Commands.CANCEL_SERVER_COMMAND, dict(cancel_cmd_id=10),
                               cmd_unique_id_third, None, session_id)
            print('***11')
            response = await self.communicator_receive_from(timeout=10)
            print('***12')
            await self.check_response(response, 'received', cmd_unique_id_third)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'progress', cmd_unique_id_third)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'error', cmd_unique_id_third, dict(result=cmd_result))

            print('***13')
            # Spremenim, da je bila komanda kreirana že več kot en dan nazaj
            await update_command_record_async(cmd_unique_id_first, device_id,
                                              dict(completed_at=timezone.now() - timedelta(days=1)))

            # Preverim, da komande ni več
            await self.check_command_deleted(cmd_unique_id_first, device_id, timeout=2)
            print('***14')

            # Probam cancelati task za neobstoječo komando. Moram dobiti napako.
            cmd_result = dict(type='Process error', text='No Command matches the given query.')
            cmd_unique_id_third = '13|123459'
            # Potem pa pošljem komando za cancel komande, ki ne obstaja
            await send_request(communicator, Commands.CANCEL_SERVER_COMMAND, dict(cancel_cmd_id=99),
                               cmd_unique_id_third, None, session_id)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'received', cmd_unique_id_third)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'progress', cmd_unique_id_third)
            response = await self.communicator_receive_from(timeout=10)
            await self.check_response(response, 'error', cmd_unique_id_third, dict(result=cmd_result))
            print('***15')

        except Exception as e:
            exception = e

        await self.close_communicator()
        print('***16')
        if exception:
            raise exception

# TODO: DC - Če ne bo uredu...
#  - Poglej tole: https://github.com/redis/redis-py/issues/968
#  - probaj šeenkrat z preforkom...
#  - probaj šeenkrat da za vsak test ubiješ workerje in jih ponovno vzpostaviš (in prej zbrišeš cache)
#
