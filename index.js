const spawn = require('child_process').spawn
const readline = require('readline')
const log = require('winston')
const mqtt = require('mqtt')

const MQTT_BROKER = 'mqtt://192.168.0.186'

const idToName = {
  8: "ulkona",
  66: "vesimittari",
  164: "olohuone",
  246: "varasto",
}

const ridToName = {
  43: "verstas",
  190: "aula"
}

const gidToName = {
  11515: "etuovi"
}

startRtl_433()
const mqttClient = startMqttClient(MQTT_BROKER)

function startRtl_433() {
  const rtl_433 = spawn('rtl_433', ['-F', 'json'])
  const stdout = readline.createInterface({input: rtl_433.stdout})
  stdout.on('line', handleLine)
}

function handleLine(line) {
  try {
    const json = JSON.parse(line)
    handleInputJson(json)
  } catch(e) {
    log.info('Failed to parse input line:', line, e)
  }
}

function handleInputJson(json) {
  switch (json.model) {
    case 'THGR968':
    case 'THGR122N':
    case 'Nexus Temperature/Humidity':
      handleThgrOrNexus(json)
      break
    case 'Prologue sensor':
      handlePrologue(json)
      break
    case 'Generic Remote':
      handleGenericRemote(json)
      break
    default:
      log.warn('Got unknown message', json)
  }
}

function handleThgrOrNexus(json) {
  const instance = idToName[json.id]
  if(!instance) {
    log.warn('No instance mapping for rtl_433 ID', json.id)
    return
  }
  mqttClient.publish(`/sensor/${instance}`, JSON.stringify({ temperature: json.temperature_C, humidity: json.humidity, battery: json.battery, ts: new Date() }), { retain: true })
}

function handlePrologue(json) {
  const instance = ridToName[json.rid]
  if(!instance) {
    log.warn('No instance mapping for rtl_433 ID', json.rid)
    return
  }
  mqttClient.publish(`/sensor/${instance}`, JSON.stringify({ temperature: json.temperature_C, humidity: json.humidity, battery: json.battery, ts: new Date() }), { retain: true })
}

function handleGenericRemote(json) {
  const instance = gidToName[json.id]
  if(!instance) {
    log.warn('No instance mapping for rtl_433 ID', json.id)
    return
  }
  if (json.cmd == '10') {
    mqttClient.publish(`/sensor/${instance}`, JSON.stringify({ action: opened, ts: new Date() }), { retain: true })
  } else if (json.cmd == '14') {
    mqttClient.publish(`/sensor/${instance}`, JSON.stringify({ action: closed, ts: new Date() }), { retain: true })
  } else {
    log.warn('Unknown switch state', json.cmd)
    return
  }
}

function startMqttClient(brokerUrl) {
  const client = mqtt.connect(brokerUrl, { queueQoSZero : false })
  client.on('connect', () => log.info("Connected to MQTT server"))
  client.on('offline', () => log.info('Disconnected from MQTT server'))
  client.on('error', () => log.error('MQTT client error', e))
  return client
}
