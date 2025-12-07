'use strict';

const express = require('express');
const mustache = require('mustache');
const fs = require('fs');
const { Kafka } = require('kafkajs');
require('dotenv').config();

const hbase = require('hbase');
const app = express();

// Server config from CLI args (Configured on IntelliJ as 3000 and HBase URL)
const port = Number(process.argv[2]);
const url = new URL(process.argv[3]);

console.log("Connecting to HBase at:", url.href);

// HBase CLIENT
var hclient = hbase({
    host: url.hostname,
    port: url.port,
    protocol: url.protocol.replace(":", ""),
    path: url.pathname ?? "/",
    encoding: 'latin1'
});

// Kafka Producer Setup
const kafka = new Kafka({
    clientId: 'wuh-fifa-app',
    brokers: [
        process.env.KAFKA_BROKER ||
        'boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196'
    ],
    ssl: true,
    sasl: {
        mechanism: 'scram-sha-512',
        username: 'mpcs53014-2025',
        password: 'A3v4rd4@ujjw'
    }
});

const producer = kafka.producer();

(async () => {
    try {
        await producer.connect();
        console.log("Kafka producer connected.");
    } catch (err) {
        console.error("Kafka producer connection failed:", err);
    }
})();

// Helper functions to decode HBase cell values
function cellValueToNumber(val) {
    if (val == null) return 0;

    if (typeof val === 'number') return val;

    if (typeof val === 'string') {
        const trimmed = val.trim();
        if (/^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed);
        val = Buffer.from(val, 'latin1');
    }

    if (Buffer.isBuffer(val)) {
        try {
            if (val.length === 4) {
                // 32-bit int (big-endian)
                // Specifically for vals submited from Kafka, decode them here. 
                return val.readInt32BE(0);
            }
            if (val.length === 8) {
                const dbl = val.readDoubleBE(0);
                if (!isNaN(dbl)) return dbl;
                return Number(val.readBigInt64BE(0));
            }
        } catch (e) {
            console.error("Decode error:", e);
        }
    }
    return 0;
}

function rowToMap(row) {
    const out = {};
    row.forEach(col => {
        let cf = col.column.split(':')[1];
        out[cf] = cellValueToNumber(col['$']);
    });
    return out;
}

app.use(express.static('public'));

// SUBMIT EVENT (write to Kafka)
app.get('/submit_event', async (req, res) => {
    const country = (req.query.country || '').trim();
    const newRank = Number(req.query.new_rank);
    const points = Number(req.query.points);
    const ts = Date.now();

    if (!country || Number.isNaN(newRank) || Number.isNaN(points)) {
        return res.send("<h2 style='color:red;text-align:center'>Invalid inputs.</h2>");
    }

    const event = { country, new_rank: newRank, points, ts };

    // Return the html response after sending to Kafka
    try {
        await producer.send({
            topic: 'wuh_fifa_events',
            messages: [{ value: JSON.stringify(event) }]
        });

        res.send(`
            <h2 style="text-align:center;color:green">Event Sent to Kafka!</h2>
            <pre style="width:50%;margin:0 auto;background:#f0f0f0;padding:10px;">
            ${JSON.stringify(event, null, 2)}
            </pre>
            <p style="text-align:center"><a href="/">Back</a></p>
        `);
    } catch (err) {
        console.error("Error sending to Kafka:", err);
        res.send("<h2 style='color:red;text-align:center'>Failed to send event to Kafka.</h2>");
    }
});

// LIST AVAILABLE COUNTRIES
app.get('/countries', (req, res) => {
    const rows = [];

    hclient.table('wuh_fifa_team_stats')
        .scan({ maxVersions: 1 }, (err, cells) => {
            if (err) {
                console.error(err);
                return res.send("<h2 style='color:red;text-align:center'>HBase error while scanning countries.</h2>");
            }

            const seen = new Set();
            cells.forEach(cell => {
                if (!seen.has(cell.key)) {
                    seen.add(cell.key);
                    rows.push(cell.key);
                }
            });

            rows.sort();

            let html = `
                <html><head>
                <title>Available Countries</title>
                <link rel="stylesheet" href="/elegant-aero.css">
                </head><body style="background:white;">
                <h2 style="text-align:center;">Available Countries</h2>
                <div style="width:50%;margin:0 auto;background:white;padding:20px;" class="elegant-aero">
                    <ul style="list-style:none;padding-left:0;">`;

            rows.forEach(c => {
                html += `<li style="padding:5px 0;font-size:18px;">
                            <a href="/team_stats.html?country=${encodeURIComponent(c)}">${c}</a>
                         </li>`;
            });

            html += `
                </ul>
                </div>
                <p style="text-align:center;"><a href="/">Back</a></p>
                </body></html>
            `;
            res.send(html);
        });
});

// TEAM STATS LOOKUP
app.get('/team_stats.html', (req, res) => {
    const country = (req.query.country || '').trim();

    if (!country) {
        return res.send("<h2 style='color:red;text-align:center'>Missing country name.</h2>");
    }

    hclient.table('wuh_fifa_team_stats')
        .row(country)
        .get((err, cells) => {
            if (err) {
                console.error(err);
                return res.send("<h2 style='color:red;text-align:center'>HBase lookup error.</h2>");
            }
            if (!cells || cells.length === 0) {
                return res.send(`
                    <h2 style="color:red;text-align:center">
                        '${country}' not found in FIFA dataset.
                    </h2>
                    <p style="text-align:center"><a href="/">Back</a></p>
                `);
            }

            const stats = rowToMap(cells);
            const tpl = fs.readFileSync('team_result.mustache').toString();
            res.send(mustache.render(tpl, { country, ...stats }));
        });
});

// Start server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
