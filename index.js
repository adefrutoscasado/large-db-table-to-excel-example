const etl = require('etl')
const { databaseConfig } = require('./config')
const fs = require('fs')
const { Worker } = require('worker_threads')
const knex = require('knex')(databaseConfig)

const unlink = (file) => new Promise((resolve, reject) => {
  fs.unlink(file, (error) => {
    if (error) reject(error)
    resolve()
  })
})

const formatMemoryUsage = (data) => `${Math.round(data / 1024 / 1024 * 100) / 100}`

const formatNumber = (number) => {
  return Number(Number(number).toFixed(2))
}

const logMemory = () => {
  const memoryData = process.memoryUsage()
  console.log('Memory', formatNumber(formatMemoryUsage(memoryData.heapUsed)), 'Mb')
}

const _console = {
  time(key) {
    this[key] = new Date()
  },
  timeEnd(key) {
    if (!this[key]) return console.log('No previous time saved for ', key)
    return (((new Date() - this[key]) / 1000))
  }
}


const writeFoFile = (data) => {
  return new Promise((resolve, reject) => {
    const worker = new Worker(require.resolve('./fileWriter.js'), { workerData: data })
    worker.on('message', (msg) => {
      resolve(`Message received: ${msg}`)
    })
    worker.on('error', (msg) => {
      reject(`An error ocurred: ${msg}`)
    })
  })
}


const main = async () => {
  try {
    // Delete previously saved file
    await unlink('data.xlsx')
  } catch (error) {
  }

  const stream = knex
    .select('*')
    .from('schema.your_large_table')
    // .limit(100000)
    .stream()

  _console.time('Total')
  let count = 0
  let total = 0
  let lastTotal = 0

  stream.on('data', (data) => {
    total++
    // Log memory every 1000 elements received
    if (total % 1000 === 0) {
      console.log('Collecting data, received a total of', total, 'elements')
      logMemory()
    }
  })

  // Write to file every 30.000 rows
  const WRITE_INTERVAL_ELEMENTS = 30_000

  stream
    .pipe(etl.collect(WRITE_INTERVAL_ELEMENTS))
    .pipe(etl.map(async data => {

      // Log progress
      count = count + WRITE_INTERVAL_ELEMENTS
      console.log('\n___ Writing to file at', count, ' ___')
      let timeEndTotal = _console.timeEnd('Total')
      console.log('Step took: ', formatNumber(timeEndTotal - lastTotal), ' seconds')
      console.log('Total: ', formatNumber(timeEndTotal), ' seconds')
      logMemory()

      // Write to file. Dont await this promise, we want to keep receiving data.
      // The file will be written using other javascript thread 
      writeFoFile(data)
        .then((response) => {
          console.log(response)
        })
        .catch((error) => {
          console.error(error)
        })

      lastTotal = timeEndTotal
    }))
    .promise()
    .then(() => {
      console.log('\nProcess completed')
    })
    .catch((error) => {
      console.error(error)
    })
}


main()