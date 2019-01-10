const spawn = require('child_process').spawn
const readline = require('readline')
const log = require('winston')
const mqtt = require('mqtt')

const MQTT_BROKER = 'mqtt://192.168.0.186'
const mqtt_topic = 'sensor'
const duplicateTimeOut = 500 // ms

var lastHeard = []

const idToName = {
  8: "ulkona",
  66: "vesimittari",
  164: "olohuone",
  246: "varasto"
}

const ridToName = {
  43: "verstas",
  190: "aula"
}

const gidToName = {
  11515: "etuovi"
}

const cmdToAction = {
  10: "opened",
  14: "closed"
}

startRtl_433()
const mqttClient = startMqttClient(MQTT_BROKER)

function startRtl_433() {
  const rtl_433 = spawn('rtl_433', ['-F', 'json', '-M', 'hires'])
  const stdout = readline.createInterface({input: rtl_433.stdout})
  stdout.on('line', handleLine)
}

function handleLine(line) {
  try {
    var json = JSON.parse(line)
    handleInputJson(json)
  } catch(e) {
    log.info('Failed to parse input line:', line, e)
  }
}

function handleInputJson(json) {
  json.time = new Date(json.time)

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
    log.warn('No instance mapping for THGRx or Nexus sensor ID', json.id)
    return
  }

  var msg = {}
  msg['temperature'] = json.temperature_C
  msg['humidity'] = json.humidity
  msg['battery'] = json.battery
  msg['ts'] = json.time

  mqttPublish(instance, msg)
}

function handlePrologue(json) {
  const instance = ridToName[json.rid]
  if(!instance) {
    log.warn('No instance mapping for Prologue sensor ID', json.rid)
    return
  }
  var msg = {}
  msg['temperature'] = json.temperature_C
  msg['humidity'] = json.humidity
  msg['battery'] = json.battery
  msg['ts'] = json.time

  mqttPublish(instance, msg)
}

function handleGenericRemote(json) {
  const now = json.time.getTime()
  const instance = gidToName[json.id]
  const action = cmdToAction[json.cmd]

  if(!instance || !action) {
    log.warn('No instance mapping or action for Generic Remote ID ', json.id)
    return
  }

  if (lastHeard[json.id] + duplicateTimeOut > now) {
    lastHeard[json.id] = now 
    return  // Duplicate!
  }
  lastHeard[json.id] = now

  var msg = {}
  msg['action'] = action
  msg['ts'] = json.time

  mqttPublish(instance, msg)
}

function mqttPublish(instance, msg) {
  mqttClient.publish(`/${mqtt_topic}/${instance}`, JSON.stringify(msg), { retain: true })
}

function startMqttClient(brokerUrl) {
  const client = mqtt.connect(brokerUrl, { queueQoSZero : false })
  client.on('connect', () => log.info("Connected to MQTT server"))
  client.on('offline', () => log.info('Disconnected from MQTT server'))
  client.on('error', () => log.error('MQTT client error', e))
  return client
}