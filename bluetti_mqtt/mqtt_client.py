import asyncio
from dataclasses import dataclass
from enum import auto, Enum, unique
import json
import logging
import re
from typing import List, Optional
from aiomqtt import Client, MqttError
from paho.mqtt.client import MQTTMessage
from bluetti_mqtt.bus import CommandMessage, EventBus, ParserMessage
from bluetti_mqtt.core import BluettiDevice, DeviceCommand
from prometheus_client import Gauge


@unique
class MqttFieldType(Enum):
    NUMERIC = auto()
    BOOL = auto()
    ENUM = auto()
    BUTTON = auto()


@dataclass(frozen=True)
class MqttFieldConfig:
    type: MqttFieldType
    setter: bool
    advanced: bool  # Do not export by default to Home Assistant
    home_assistant_extra: dict
    id_override: Optional[str] = None  # Used to override Home Assistant field id


COMMAND_TOPIC_RE = re.compile(r'^bluetti/command/([\w ]+)-(\d+)/([a-z_]+)$')
NORMAL_DEVICE_FIELDS = {
    'dc_input_power': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Input Power',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'ac_input_power': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'AC Input Power',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'ac_output_power': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'AC Output Power',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'dc_output_power': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Output Power',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'power_generation': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'Total Power Generation',
            'unit_of_measurement': 'kWh',
            'device_class': 'energy',
            'state_class': 'total_increasing',
        }
    ),
    'total_battery_percent': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'Total Battery Percent',
            'unit_of_measurement': '%',
            'device_class': 'battery',
            'state_class': 'measurement',
        }
    ),
    'total_battery_charge_time': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'Time To Full Charge',
            'unit_of_measurement': 'min',
            'device_class': 'duration',
            'state_class': 'measurement',
        }
    ),
    'total_battery_discharge_time': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'Time Remaining',
            'unit_of_measurement': 'min',
            'device_class': 'duration',
            'state_class': 'measurement',
        }
    ),
    'ac_output_on': MqttFieldConfig(
        type=MqttFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'AC Output',
            'device_class': 'outlet',
        }
    ),
    'dc_output_on': MqttFieldConfig(
        type=MqttFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Output',
            'device_class': 'outlet',
        }
    ),
    'ac_output_mode': MqttFieldConfig(
        type=MqttFieldType.ENUM,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'AC Output Mode',
        }
    ),
    'internal_ac_voltage': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal AC Voltage',
            'unit_of_measurement': 'V',
            'device_class': 'voltage',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_current_one': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Current Sensor 1',
            'unit_of_measurement': 'A',
            'device_class': 'current',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_power_one': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Power Sensor 1',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_ac_frequency': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal AC Frequency',
            'unit_of_measurement': 'Hz',
            'device_class': 'frequency',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_current_two': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Current Sensor 2',
            'unit_of_measurement': 'A',
            'device_class': 'current',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_power_two': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Power Sensor 2',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'ac_input_voltage': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'AC Input Voltage',
            'unit_of_measurement': 'V',
            'device_class': 'voltage',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_current_three': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Current Sensor 3',
            'unit_of_measurement': 'A',
            'device_class': 'current',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_power_three': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Power Sensor 3',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'ac_input_frequency': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'AC Input Frequency',
            'unit_of_measurement': 'Hz',
            'device_class': 'frequency',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'total_battery_voltage': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Total Battery Voltage',
            'unit_of_measurement': 'V',
            'device_class': 'voltage',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'total_battery_current': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Total Battery Current',
            'unit_of_measurement': 'A',
            'device_class': 'current',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'ups_mode': MqttFieldConfig(
        type=MqttFieldType.ENUM,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'UPS Working Mode',
            'options': ['CUSTOMIZED', 'PV_PRIORITY', 'STANDARD', 'TIME_CONTROL'],
        }
    ),
    'split_phase_on': MqttFieldConfig(
        type=MqttFieldType.BOOL,
        setter=False,  # For safety purposes, I'm not exposing this as a setter
        advanced=False,
        home_assistant_extra={
            'name': 'Split Phase',
        }
    ),
    'split_phase_machine_mode': MqttFieldConfig(
        type=MqttFieldType.ENUM,
        setter=False,  # For safety purposes, I'm not exposing this as a setter
        advanced=False,
        home_assistant_extra={
            'name': 'Split Phase Machine',
        }
    ),
    'grid_charge_on': MqttFieldConfig(
        type=MqttFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Grid Charge',
        }
    ),
    'time_control_on': MqttFieldConfig(
        type=MqttFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Time Control',
        }
    ),
    'battery_range_start': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Battery Range Start',
            'step': 1,
            'min': 0,
            'max': 100,
            'unit_of_measurement': '%',
        }
    ),
    'battery_range_end': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'Battery Range End',
            'step': 1,
            'min': 0,
            'max': 100,
            'unit_of_measurement': '%',
        }
    ),
    'max_grid_charge_current': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Max Grid Charge Current',
            'step': 1,
            'min': 1,
            'max': 10,
            'unit_of_measurement': 'A',
        }
    ),
    'led_mode': MqttFieldConfig(
        type=MqttFieldType.ENUM,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'LED Mode',
            'icon': 'mdi:lightbulb',
            'options': ['LOW', 'HIGH', 'SOS', 'OFF'],
        }
    ),
    'power_off': MqttFieldConfig(
        type=MqttFieldType.BUTTON,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Power Off',
            'payload_press': 'ON',
        }
    ),
    'auto_sleep_mode': MqttFieldConfig(
        type=MqttFieldType.ENUM,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Screen Auto Sleep Mode',
            'icon': 'mdi:sleep',
            'options': ['THIRTY_SECONDS', 'ONE_MINUTE', 'FIVE_MINUTES', 'NEVER'],
        }
    ),
    'eco_on': MqttFieldConfig(
        type=MqttFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'ECO',
            'icon': 'mdi:sprout',
        }
    ),
    'dc_eco_on': MqttFieldConfig(
        type=MqttFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'DC_ECO',
            'icon': 'mdi:sprout',
        }
    ),
    'ac_eco_on': MqttFieldConfig(
        type=MqttFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'AC_ECO',
            'icon': 'mdi:sprout',
        }
    ),
    'eco_shutdown': MqttFieldConfig(
        type=MqttFieldType.ENUM,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'ECO Shutdown',
            'icon': 'mdi:sprout',
            'options': ['ONE_HOUR', 'TWO_HOURS', 'THREE_HOURS', 'FOUR_HOURS'],
        }
    ),
    'charging_mode': MqttFieldConfig(
        type=MqttFieldType.ENUM,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Charging Mode',
            'icon': 'mdi:battery-charging',
            'options': ['STANDARD', 'SILENT', 'TURBO'],
        }
    ),
    'power_lifting_on': MqttFieldConfig(
        type=MqttFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Power Lifting',
            'icon': 'mdi:arm-flex',
        }
    ),
}
DC_INPUT_FIELDS = {
    'dc_input_voltage1': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Input Voltage 1',
            'unit_of_measurement': 'V',
            'device_class': 'voltage',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'dc_input_power1': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Input Power 1',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'dc_input_current1': MqttFieldConfig(
        type=MqttFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Input Current 1',
            'unit_of_measurement': 'A',
            'device_class': 'current',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
}
PROMETHEUS_FIELDS = {
    'dc_input_power': Gauge('bluetti_dc_input_power','DC input power'),
    'ac_input_power': Gauge('bluetti_ac_input_power','AC input power'),
    'ac_output_power': Gauge('bluetti_ac_output_power','AC output power'),
    'dc_output_power': Gauge('bluetti_dc_output_power','DC output power'),
    'power_generation': Gauge('bluetti_power_generation','Power generation'),
    'total_battery_percent': Gauge('bluetti_total_battery_percent','Total battery percent'),
    'total_battery_charge_time': Gauge('bluetti_total_battery_charge_time','Total battery charge time'),
    'total_battery_discharge_time': Gauge('bluetti_total_battery_discharge_time','Total battery discharge time'),
    'ac_output_on': Gauge('bluetti_ac_output_on','AC output on'),
    'dc_output_on': Gauge('bluetti_dc_output_on','DC output on'),
    'ac_output_mode': Gauge('bluetti_ac_output_mode','AC output mode'),
    'internal_ac_voltage': Gauge('bluetti_internal_ac_voltage','Internal AC voltage'),
    'internal_current_one': Gauge('bluetti_internal_current_one','Internal current one'),
    'internal_power_one': Gauge('bluetti_internal_power_one','Internal power one'),
    'internal_ac_frequency': Gauge('bluetti_internal_ac_frequency','Internal AC frequency'),
    'internal_current_two': Gauge('bluetti_internal_current_two','Internal current two'),
    'internal_power_two': Gauge('bluetti_internal_power_two','Internal power two'),
    'ac_input_voltage': Gauge('bluetti_ac_input_voltage','AC input voltage'),
    'internal_current_three': Gauge('bluetti_internal_current_three','Internal current three'),
    'internal_power_three': Gauge('bluetti_internal_power_three','Internal power three'),
    'ac_input_frequency': Gauge('bluetti_ac_input_frequency','AC input frequency'),
    'total_battery_voltage': Gauge('bluetti_total_battery_voltage','Total battery voltage'),
    'total_battery_current': Gauge('bluetti_total_battery_current','Total battery current'),
    'ups_mode': Gauge('bluetti_ups_mode','UPS mode'),
    'split_phase_on': Gauge('bluetti_split_phase_on','Split phase on'),
    'split_phase_machine_mode': Gauge('bluetti_split_phase_machine_mode','Split phase machine mode'),
    'grid_charge_on': Gauge('bluetti_grid_charge_on','Grid charge on'),
    'time_control_on': Gauge('bluetti_time_control_on','Time control on'),
    'battery_range_start': Gauge('bluetti_battery_range_start','Battery range start'),
    'battery_range_end': Gauge('bluetti_battery_range_end','Battery range end'),
    'max_grid_charge_current': Gauge('bluetti_max_grid_charge_current','Max grid charge current'),
    'led_mode': Gauge('bluetti_led_mode','LED mode'),
    'power_off': Gauge('bluetti_power_off','Power off'),
    'auto_sleep_mode': Gauge('bluetti_auto_sleep_mode','Auto sleep mode'),
    'eco_on': Gauge('bluetti_eco_on','Eco on'),
    'ac_eco_on': Gauge('bluetti_ac_eco_on','AC Eco on'),
    'dc_eco_on': Gauge('bluetti_dc_eco_on','DC Eco on'),
    'eco_shutdown': Gauge('bluetti_eco_shutdown','Eco shutdown'),
    'charging_mode': Gauge('bluetti_charging_mode','Charging mode'),
    'power_lifting_on': Gauge('bluetti_power_lifting_on','Power lifting on'),
    'dc_input_voltage1': Gauge('bluetti_dc_input_voltage1','DC input voltage1'),
    'dc_input_power1': Gauge('bluetti_dc_input_power1','DC input power1'),
    'dc_input_current1': Gauge('bluetti_dc_input_current1','DC input current1'),
    'pack_voltage': Gauge('bluetti_pack_voltage','Pack voltage',['pack_num']),
    'pack_battery_percent': Gauge('bluetti_pack_battery_percent','Pack battery percent',['pack_num']),
    'internal_dc_input_voltage': Gauge('bluetti_internal_dc_input_voltage','Internal DC input voltage'),
    'internal_dc_input_current': Gauge('bluetti_internal_dc_input_current','Internal DC input current'),
    'internal_dc_input_power': Gauge('bluetti_internal_dc_input_power','Internal DC input power')
}


def battery_pack_fields(pack: int):
    return {
        'pack_status': MqttFieldConfig(
            type=MqttFieldType.ENUM,
            setter=False,
            advanced=True,
            home_assistant_extra={
                'name': f'Battery Pack {pack} Status',
                'value_template': '{{ value_json.status }}'
            },
            id_override=f'pack_status{pack}'
        ),
        'pack_voltage': MqttFieldConfig(
            type=MqttFieldType.NUMERIC,
            setter=False,
            advanced=True,
            home_assistant_extra={
                'name': f'Battery Pack {pack} Voltage',
                'unit_of_measurement': 'V',
                'device_class': 'voltage',
                'state_class': 'measurement',
                'force_update': True,
                'value_template': '{{ value_json.voltage }}'
            },
            id_override=f'pack_voltage{pack}'
        ),
        'pack_battery_percent': MqttFieldConfig(
            type=MqttFieldType.NUMERIC,
            setter=False,
            advanced=False,
            home_assistant_extra={
                'name': f'Battery Pack {pack} Percent',
                'unit_of_measurement': '%',
                'device_class': 'battery',
                'state_class': 'measurement',
                'value_template': '{{ value_json.percent }}'
            },
            id_override=f'pack_percent{pack}'
        ),
    }


class MQTTClient:
    devices: List[BluettiDevice]
    message_queue: asyncio.Queue

    def __init__(
        self,
        bus: EventBus,
        hostname: str,
        home_assistant_mode: str,
        port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.bus = bus
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.home_assistant_mode = home_assistant_mode
        self.devices = []

    async def run(self):
        while True:
            logging.info('Connecting to MQTT broker...')
            try:
                async with Client(
                    hostname=self.hostname,
                    port=self.port,
                    username=self.username,
                    password=self.password
                ) as client:
                    logging.info('Connected to MQTT broker')

                    # Connect to event bus
                    self.message_queue = asyncio.Queue()
                    self.bus.add_parser_listener(self.handle_message)

                    # Handle pub/sub
                    await asyncio.gather(
                        self._handle_commands(client),
                        self._handle_messages(client)
                    )
            except MqttError:
                logging.exception('MQTT error:')
                await asyncio.sleep(5)

    async def handle_message(self, msg: ParserMessage):
        await self.message_queue.put(msg)

    async def _handle_commands(self, client: Client):
        await client.subscribe('bluetti/command/#')
        async for mqtt_message in client.messages:
            await self._handle_command(mqtt_message)

    async def _handle_messages(self, client: Client):
        while True:
            msg: ParserMessage = await self.message_queue.get()
            if msg.device not in self.devices:
                await self._init_device(msg.device, client)
            await self._handle_message(client, msg)
            self.message_queue.task_done()

    async def _init_device(self, device: BluettiDevice, client: Client):
        # Register device
        self.devices.append(device)

        # Skip announcing device to Home Assistant if disabled
        if self.home_assistant_mode == 'none':
            return

        def payload(id: str, device: BluettiDevice, field: MqttFieldConfig) -> str:
            ha_id = id if not field.id_override else field.id_override
            payload_dict = {
                'state_topic': f'bluetti/state/{device.type}-{device.sn}/{id}',
                'device': {
                    'identifiers': [
                        f'{device.sn}'
                    ],
                    'manufacturer': 'Bluetti',
                    'name': f'{device.type} {device.sn}',
                    'model': device.type
                },
                'unique_id': f'{device.sn}_{ha_id}',
                'object_id': f'{device.type}_{ha_id}',
            }
            if field.setter:
                payload_dict['command_topic'] = f'bluetti/command/{device.type}-{device.sn}/{id}'
            payload_dict.update(field.home_assistant_extra)

            return json.dumps(payload_dict, separators=(',', ':'))

        # Publish normal fields
        for name, field in NORMAL_DEVICE_FIELDS.items():
            # Skip fields not supported by the device
            if not device.has_field(name):
                continue

            # Skip advanced fields if not enabled
            if field.advanced and self.home_assistant_mode != 'advanced':
                continue

            # Figure out Home Assistant type
            if field.type == MqttFieldType.NUMERIC:
                type = 'number' if field.setter else 'sensor'
            elif field.type == MqttFieldType.BOOL:
                type = 'switch' if field.setter else 'binary_sensor'
            elif field.type == MqttFieldType.ENUM:
                type = 'select' if field.setter else 'sensor'
            elif field.type == MqttFieldType.BUTTON:
                type = 'button'

            # Publish config
            await client.publish(
                f'homeassistant/{type}/{device.sn}_{name}/config',
                payload=payload(name, device, field).encode(),
                retain=True
            )

        # Publish battery pack configs
        for pack in range(1, device.pack_num_max + 1):
            fields = battery_pack_fields(pack)
            for name, field in fields.items():
                # Skip fields not supported by the device
                if not device.has_field(name):
                    continue

                # Publish config
                await client.publish(
                    f'homeassistant/sensor/{device.sn}_{field.id_override}/config',
                    payload=payload(f'pack_details{pack}', device, field).encode(),
                    retain=True
                )

        # Publish DC input config
        if device.has_field('internal_dc_input_voltage'):
            for name, field in DC_INPUT_FIELDS.items():
                await client.publish(
                    f'homeassistant/sensor/{device.sn}_{name}/config',
                    payload=payload(name, device, field).encode(),
                    retain=True
                )

        logging.info(f'Sent discovery message of {device.type}-{device.sn} to Home Assistant')

    async def _handle_command(self, mqtt_message: MQTTMessage):
        # Parse the mqtt_message.topic
        m = COMMAND_TOPIC_RE.match(mqtt_message.topic.value)
        if not m:
            logging.warn(f'unknown command topic: {mqtt_message.topic.value}')
            return

        # Find the matching device for the command
        device = next((d for d in self.devices if d.type == m[1] and d.sn == m[2]), None)
        if not device:
            logging.warn(f'unknown device: {m[1]} {m[2]}')
            return

        # Check if the device supports setting this field
        if not device.has_field_setter(m[3]):
            logging.warn(f'Received command for unknown topic: {m[3]} - {mqtt_message.topic}')
            return

        cmd: DeviceCommand = None
        if m[3] in NORMAL_DEVICE_FIELDS:
            field = NORMAL_DEVICE_FIELDS[m[3]]
            if field.type == MqttFieldType.ENUM:
                value = mqtt_message.payload.decode('ascii')
                cmd = device.build_setter_command(m[3], value)
            elif field.type == MqttFieldType.BOOL or field.type == MqttFieldType.BUTTON:
                value = mqtt_message.payload == b'ON'
                cmd = device.build_setter_command(m[3], value)
            elif field.type == MqttFieldType.NUMERIC:
                value = int(mqtt_message.payload.decode('ascii'))
                cmd = device.build_setter_command(m[3], value)
            else:
                raise AssertionError(f'unexpected enum type: {field.type}')
        else:
            logging.warn(f'Received command for unhandled topic: {m[3]} - {mqtt_message.topic}')
            return

        await self.bus.put(CommandMessage(device, cmd))

    async def _handle_message(self, client: Client, msg: ParserMessage):
        logging.debug(f'Got a message from {msg.device}: {msg.parsed}')
        topic_prefix = f'bluetti/state/{msg.device.type}-{msg.device.sn}/'

        # Publish normal fields
        for name, value in msg.parsed.items():
            # Skip unconfigured fields
            if name not in NORMAL_DEVICE_FIELDS:
                continue

            # Build payload string
            field = NORMAL_DEVICE_FIELDS[name]
            if field.type == MqttFieldType.NUMERIC:
                payload = str(value)
            elif field.type == MqttFieldType.BOOL or field.type == MqttFieldType.BUTTON:
                payload = 'ON' if value else 'OFF'
            elif field.type == MqttFieldType.ENUM:
                payload = value.name
            else:
                assert False, f'Unhandled field type: {field.type.name}'

            await client.publish(topic_prefix + name, payload=payload.encode())

            # Publish prometheus field
            if (field.type in [MqttFieldType.NUMERIC]):
                PROMETHEUS_FIELDS[name].set(payload)


        # Publish battery pack data
        pack_details = self._build_pack_details(msg.parsed)
        if 'pack_num' in msg.parsed and len(pack_details) > 0:
            await client.publish(
                topic_prefix + f'pack_details{msg.parsed["pack_num"]}',
                payload=json.dumps(pack_details, separators=(',', ':')).encode()
            )
            # Publish prometheus field
            if ('percent' in pack_details.keys()): PROMETHEUS_FIELDS['pack_battery_percent'].labels(msg.parsed["pack_num"]).set(pack_details['percent'])
            if ('voltage' in pack_details.keys()): PROMETHEUS_FIELDS['pack_voltage'].labels(msg.parsed["pack_num"]).set(pack_details['voltage'])


        # Publish DC input data
        if 'internal_dc_input_voltage' in msg.parsed:
            await client.publish(
                topic_prefix + 'dc_input_voltage1',
                payload=str(msg.parsed['internal_dc_input_voltage']).encode()
            )
            # Publish prometheus field
            PROMETHEUS_FIELDS['internal_dc_input_voltage'].set(payload)
        if 'internal_dc_input_power' in msg.parsed:
            await client.publish(
                topic_prefix + 'dc_input_power1',
                payload=str(msg.parsed['internal_dc_input_power']).encode()
            )
            # Publish prometheus field
            PROMETHEUS_FIELDS['internal_dc_input_power'].set(payload)
        if 'internal_dc_input_current' in msg.parsed:
            await client.publish(
                topic_prefix + 'dc_input_current1',
                payload=str(msg.parsed['internal_dc_input_current']).encode()
            )
            # Publish prometheus field
            PROMETHEUS_FIELDS['internal_dc_input_current'].set(payload)

    def _build_pack_details(self, parsed: dict):
        details = {}
        if 'pack_status' in parsed:
            details['status'] = parsed['pack_status'].name
        if 'pack_battery_percent' in parsed:
            details['percent'] = parsed['pack_battery_percent']
        if 'pack_voltage' in parsed:
            details['voltage'] = float(parsed['pack_voltage'])
        if 'cell_voltages' in parsed:
            details['voltages'] = [float(d) for d in parsed['cell_voltages']]
        return details
