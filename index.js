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
const mqtt = require('mqtt')

// Sensor definitions
const SENSORS = require('./sensors.json')

// Receiver params
const RTL433_PARAMS = process.env.RTL433_PARAMS || undefined

// MQTT broker info
const MQTT_BROKER = process.env.MQTT_BROKER ? process.env.MQTT_BROKER : 'mqtt://localhost'
const MQTT_USERNAME = process.env.MQTT_USERNAME || undefined
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || undefined
const MQTT_TOPIC = process.env.MQTT_TOPIC ? process.env.MQTT_TOPIC : 'rtl_433'

var prevMsg = {}  // keeping record when sensors were last heard

// Compile list of  protocols to listen
var protocols = []

for (var i = 0; i < SENSORS.length; i++) {
  if (SENSORS[i].protocol) {
    const protocol = SENSORS[i].protocol
    if (protocols.indexOf(protocol) === -1) { protocols.push(protocol) }
  } else {
    console.log("Sensor without protocol!")
  }
}

// Compile list of all the fields sensors will use to id themselves
const id_fields = SENSORS.reduce((ids, sensor) => {
  Object.keys(sensor.idMap).forEach( id => {
    if (ids.indexOf(id) === -1) { ids.push(id) }
  })
  return ids
}, [])

startRtl_433(protocols)
const mqttClient = startMqttClient(MQTT_BROKER, MQTT_USERNAME, MQTT_PASSWORD)

function startRtl_433(protocols) {
  var options = ['-F', 'json', '-M', 'hires']

  if (RTL433_PARAMS) {
    options = options.concat(RTL433_PARAMS.split(/\s+/))
  }

  for (i = 0; i < protocols.length; i++) {
    options = options.concat(['-R', protocols[i]])
  }

  console.log("starting rtl_433 with options: " + options)

  const rtl_433 = spawn('rtl_433', options)
  const stdout = readline.createInterface({input: rtl_433.stdout})
  stdout.on('line', handleLine)

  rtl_433.stderr.on('data', (data) => {
    console.error(`rtl_433 stderr:\n${data}`);
  });

  rtl_433.on('exit', function (code, signal) {
    console.error('rtl_433 died ' +
                `code ${code} and signal ${signal}`);
  });
}

function handleLine(line) {
  try {
    handleReceived(JSON.parse(line))
  } catch(e) {
    console.log('Failed to parse input line: ' + line + ' - ' + e)
  }
}

function handleReceived(msg) {
  var recMsg = msg
  recMsg.time = new Date(recMsg.time)
  const now = recMsg.time.getTime()

  var sensor = idSensor(id_fields, recMsg)

  // Don't repeat the message if the exact same was received within mask period (ms)
  if ('repeatMask' in sensor && prevMsg[sensor.name]) {
    if (prevMsg[sensor.name].time + sensor.repeatMask > now) {
      var { time, ...prevData} = prevMsg[sensor.name]
      var { time, ...recData} = recMsg

      if (JSON.stringify(prevData) === JSON.stringify(recData)) {
        prevMsg[sensor.name].time = now
        return
      }
    }
  }
  prevMsg[sensor.name] = recMsg
  prevMsg[sensor.name].time = now

  var data = handleData(sensor.dataMap, recMsg)

  if ('translations' in sensor) {
    data = handleTranslation(sensor.translations, data)
  }

  mqttPublish(sensor.name, data, sensor.retain)
}

function handleTranslation(translations, data) {
  Object.keys(translations).forEach( entry => {
    if (translations[entry][data[entry]]) {
      data[entry] = translations[entry][data[entry]]
    } else {
      throw "Don't know how to translate that!"
    }
  })
  return data
}

function handleData(dataMap, rec) {
  var data = {}
  data["time"] = rec.time
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
    throw "Unknown sensor."
  }
  if (matches.length > 1) {
    throw "Multiple matching sensor definitions!"
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

function mqttPublish(instance, msg, retain) {
  mqttClient.publish(`/${MQTT_TOPIC}/${instance}`, JSON.stringify(msg), { retain: retain })
}

function startMqttClient(brokerUrl, username, password) {
  const client = mqtt.connect(brokerUrl, { queueQoSZero : false, username, password })
  client.on('connect', () => console.log("Connected to MQTT server"))
  client.on('offline', () => console.log('Disconnected from MQTT server'))
  client.on('error', e => console.log('MQTT client error', e))
  return client
}