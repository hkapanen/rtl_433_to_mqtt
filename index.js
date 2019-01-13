/*
 * Copyright 2019 Harri Kapanen <harri.kapanen@iki.fi>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

const spawn = require('child_process').spawn
const readline = require('readline')
const log = require('winston')
const mqtt = require('mqtt')

// Sensor definitions
const SENSORS = require('./sensors.json')

const MQTT_BROKER = 'mqtt://192.168.0.186'
const mqtt_topic = 'sensor'

// Compile list of all the fields sensors will use to id themselves
const id_fields = SENSORS.reduce((ids, sensor) => {
  Object.keys(sensor.idMap).forEach( id => {
    if (ids.indexOf(id) === -1) { ids.push(id) }
  })
  return ids
}, [])

startRtl_433()
const mqttClient = startMqttClient(MQTT_BROKER)

function startRtl_433() {
  const rtl_433 = spawn('rtl_433', ['-F', 'json', '-M', 'hires'])
  const stdout = readline.createInterface({input: rtl_433.stdout})
  stdout.on('line', handleLine)
}

function handleLine(line) {
  try {
    var rec = JSON.parse(line)
    handleReceived(rec)
  } catch(e) {
    log.info('Failed to parse input line:', line, e)
  }
}

function handleReceived(rec) {
  var sensor = idSensor(id_fields, rec)
  var data = handleData(sensor.dataMap, rec)

  mqttPublish(sensor.name, data)
}

function handleData(dataMap, rec) {
  var timestamp = new Date(rec.time)
  var data = {}

  data["time"] = timestamp.toISOString()
  Object.keys(dataMap).forEach( key => {
    data[key] = rec[dataMap[key]]
  })

  return data
}

function idSensor(id_fields, rec) {
  // Check what fields the received message carries that can be used as an id
  var common_ids = Object.keys(rec).filter(x => id_fields.includes(x))

  // Find values for identity defining fields
  var identity = common_ids.reduce((obj, key) => ({ ...obj, [key]: rec[key] }), {})

  // Find sensor(s) that match those fields
  var matches = SENSORS.filter(sensor => (idMatch(sensor.idMap, identity)), [])

  if (matches.length == 0) {
   console.log('Received message from unknown sensor.')
  }
  if (matches.length > 1) {
    console.log('Received message matches multiple sensor definitions!')
  }
  return matches[0]
}

function idMatch(id1, id2) {
  var match = true
  Object.keys(id1).forEach( key => {
    if (id1[key] != id2[key]) {
      match = false
    }    
  })
  return match
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