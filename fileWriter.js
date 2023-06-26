const { workerData, parentPort } = require('worker_threads')
const excelJs = require('exceljs')

function flatten(obj) {
    const result = {}
    for (const key of Object.keys(obj)) {
        if (typeof obj[key] === 'object') {
            const nested = flatten(obj[key])
            for (const nestedKey of Object.keys(nested)) {
                result[`${key}.${nestedKey}`] = nested[nestedKey]
            }
        } else {
            result[key] = obj[key]
        }
    }
    return result
}

const FILE = 'data.xlsx'

const writeToFile = async (data) => {
    const workbook = new excelJs.Workbook()

    try {
        await workbook.xlsx.readFile(FILE)
    } catch (error) {
        // if not exists yet, create the worksheet
        workbook.addWorksheet('Data')
    }

    const worksheet = workbook.getWorksheet('Data')
    if (data.length > 0) {
        worksheet.columns = Object.keys(flatten(data[0])).map((key) => ({
            header: key,
            key,
        }))
    }
    worksheet.addRows(data.map(flatten))
    await workbook.xlsx.writeFile(FILE)
    parentPort.postMessage(`Finished writing to file`)
}

writeToFile(workerData)