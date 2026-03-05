const sql = require("mssql")
const fs = require("fs")
require("dotenv").config()

// servidor donde estan las empresas
const configA = {
    user: process.env.DBA_USER,
    password: process.env.DBA_PASSWORD,
    server: process.env.DBA_SERVER,
    database: "master",
    options: {
        encrypt: false,
        trustServerCertificate: true
    }
}

// servidor donde esta INT_config_egresos
const configB = {
    user: process.env.DBB_USER,
    password: process.env.DBB_PASSWORD,
    server: process.env.DBB_SERVER,
    database: process.env.DBB_DATABASE,
    options: {
        encrypt: false,
        trustServerCertificate: true
    }
}

const empresas = [
    { bd: "ct2021_EL_POTRERO", tabla: "Egresos" },
    { bd: "ct2021_EL_POTRERO", tabla: "Cheques" },
    { bd: "ct2021POTRERO_CABORCA", tabla: "Egresos" },
    { bd: "ct2021POTRERO_CABORCA", tabla: "Cheques" },
    { bd: "ct2021_SRA", tabla: "Egresos" },
    { bd: "ct2021_SRA", tabla: "Cheques" },
    { bd: "ct2021_MURRINAVA_INM", tabla: "Egresos" },
    { bd: "ct2021_MURRINAVA_INM", tabla: "Cheques" }
]

const fechaLimite = "2025-09-01"

async function generarQuery(poolA, poolB, bd, tabla) {

    console.log(`Procesando ${bd} ${tabla}`)

    // 1 obtener ids desde servidor A
    const idsResult = await poolA.request().query(`
        SELECT Id
        FROM [${bd}].dbo.[${tabla}]
        WHERE Fecha >= '2025-08-01'
        AND Fecha < '2026-01-01'
        ORDER BY Id
    `)

    const ids = idsResult.recordset.map(r => r.Id)

    if (ids.length === 0) return ""

    // 2 obtener ids ya existentes en servidor B
    const existentesResult = await poolB.request()
        .input("bd", sql.VarChar, bd)
        .input("tabla", sql.VarChar, tabla)
        .query(`
            SELECT tabla_idregistro
            FROM INT_config_egresos
            WHERE basedatos_empresa = @bd
            AND tabla = @tabla
            AND status = 1
        `)

    const existentes = new Set(
        existentesResult.recordset.map(r => r.tabla_idregistro)
    )

    // 3 filtrar ids nuevos
    const nuevos = ids.filter(id => !existentes.has(id))

    if (nuevos.length === 0) {
        console.log("Sin registros nuevos")
        return ""
    }

    // 4 generar values
    const values = nuevos.map(id => `(${id})`).join(",")

    return `
INSERT INTO INT_config_egresos(
basedatos_empresa,
tabla,
tabla_idregistro,
autorizado,
ide_autorizado,
fecha_autorizado,
pagado,
ide_pagado,
fecha_pagado,
status
)
SELECT '${bd}','${tabla}',v.tabla_idregistro,1,1,GETDATE(),1,1,GETDATE(),1
FROM (VALUES
${values}
) v(tabla_idregistro);

`
}

async function main() {

    const poolA = await new sql.ConnectionPool(configA).connect()
    const poolB = await new sql.ConnectionPool(configB).connect()

    let queryFinal = ""

    for (const e of empresas) {

        const q = await generarQuery(poolA, poolB, e.bd, e.tabla)
        queryFinal += q

    }

    if (queryFinal.length === 0) {
        console.log("Los registros estan al dia")
        return
    } 

    // generar fecha y hora
    const now = new Date()
    const timestamp = now.toISOString()
        .replace(/T/, "_")
        .replace(/:/g, "-")
        .split(".")[0]

    fs.writeFileSync(`querys/queries_generadas_${timestamp}.sql`, queryFinal)

    console.log("Queries generadas correctamente")
}

main()