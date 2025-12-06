'use strict';

const { Kafka } = require('kafkajs');
const express = require('express');
const mustache = require('mustache');
const fs = require('fs');
require('dotenv').config();

const app = express();

// Server config (Got from IntelliJ)
const port = Number(process.argv[2]);
const url = new URL(process.argv[3]);

console.log("Connecting to HBase at:", url.href);

const hbase = require('hbase');

var hclient = hbase({
    host: url.hostname,
    port: url.port,
    protocol: url.protocol.replace(":", ""),
    path: url.pathname ?? "/",
    encoding: 'latin1'
});

// Kafka client (fill in your actual broker string from class)
const kafka = new Kafka({
    clientId: 'wuh-fifa-app',
    brokers: [process.env.KAFKA_BROKER || 'boot-publicbyg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196'],
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
        console.log("Kafka producer connected");
    } catch (e) {
        console.error("Kafka producer connection failed:", e);
    }
})();


// Helpers
function cellValueToNumber(val) {
    const intOrFloat = /^-?\d+(\.\d+)?$/;  // allow integers AND decimals

    if (typeof val === 'string') {
        // First try interpreting the string as text
        if (intOrFloat.test(val.trim())) {
            return Number(val.trim());
        }
        // Then try treating it as binary and decoding to UTF-8
        const buf = Buffer.from(val, 'latin1');
        const asText = buf.toString('utf8').trim();
        if (intOrFloat.test(asText)) {
            return Number(asText);
        }
        return 0;
    }

    if (Buffer.isBuffer(val)) {
        // Try UTF-8 text first
        const asText = val.toString('utf8').trim();
        if (intOrFloat.test(asText)) {
            return Number(asText);
        }

        // If it's an 8-byte binary number, last resort: try double
        if (val.length === 8) {
            try {
                return val.readDoubleBE(0);
            } catch {
                // fallback below
            }
        }
    }
    return Number(val) || 0;
}

function rowToMap(row) {
    const stats = {};
    row.forEach((item) => {
        stats[item.column.split(':')[1]] = cellValueToNumber(item['$']);
    });
    return stats;
}

// Static files
app.use(express.static('public'));

// Render submit-event.html
app.get('/submit_event', async (req, res) => {
    const country = (req.query['country'] || '').trim();
    const newRank = Number(req.query['new_rank']);
    const points  = Number(req.query['points']);
    const ts      = Date.now();

    if (!country || Number.isNaN(newRank) || Number.isNaN(points)) {
        return res.send("<h2 style='color:red;text-align:center'>Invalid inputs.</h2>");
    }

    const event = {
        country,
        new_rank: newRank,
        points,
        ts
    };

    try {
        await producer.send({
            topic: 'wuh_fifa_events',
            messages: [
                { value: JSON.stringify(event) }
            ],
        });

        res.send(`
            <h2 style="text-align:center;color:green">Event Sent to Kafka!</h2>
            <pre style="width:50%;margin:0 auto;background:#f0f0f0;padding:10px;">${JSON.stringify(event, null, 2)}</pre>
            <p style="text-align:center"><a href="/">Back</a></p>
        `);

    } catch (err) {
        console.error("Error sending to Kafka:", err);
        res.send("<h2 style='color:red;text-align:center'>Failed to send event to Kafka.</h2>");
    }
});

// ----------------------------------------------------
// Submit live-ranking event (speed layer later)
// ----------------------------------------------------
app.get('/submit_event', (req, res) => {
    const country = (req.query['country'] || '').trim();
    const newRank = Number(req.query['new_rank']);
    const points  = Number(req.query['points']);

    if (!country || Number.isNaN(newRank) || Number.isNaN(points)) {
        return res.send("<h2 style='color:red;text-align:center'>Invalid inputs.</h2>");
    }

    // For now: just acknowledge. Later: push to Kafka.
    res.send(`
        <h2 style="text-align:center;color:green">Event Received!</h2>
        <p style="text-align:center">
            Country: ${country}<br>
            New Rank: ${newRank}<br>
            New Points: ${points}
        </p>
        <p style="text-align:center"><a href="/">Back</a></p>
    `);
});

app.get('/countries', (req, res) => {
    const rows = [];

    hclient.table('wuh_fifa_team_stats')
        .scan({ maxVersions: 1 }, (err, cells) => {
            if (err) {
                console.error(err);
                return res.send("<h2 style='color:red;text-align:center'>HBase error while scanning countries.</h2>");
            }

            // Extract row keys ONLY
            const seen = new Set();
            cells.forEach(cell => {
                if (!seen.has(cell.key)) {
                    seen.add(cell.key);
                    rows.push(cell.key);
                }
            });

            // Sort alphabetically
            rows.sort();

            // Render a very simple HTML page
            let html = `
                <html><head>
                <title>Available Countries</title>
                <link rel="stylesheet" href="/elegant-aero.css">
                </head><body style="background:white;">
                <h2 style="text-align:center;">Available Countries in Database</h2>
                <div style="width:50%;margin:0 auto;background:white;padding:20px;" class="elegant-aero">
                    <ul style="list-style:none;padding-left:0;">`;

            rows.forEach(c => {
                html += `<li style="padding:4px 0;font-size:16px;">
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

// ----------------------------------------------------
// Lookup team stats from HBase
// ----------------------------------------------------
app.get('/team_stats.html', (req, res) => {
    const country = (req.query['country'] || '').trim();

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
                        '${country}' is not found in FIFA dataset.
                    </h2>
                    <p style="text-align:center"><a href="/">Back</a></p>
                `);
            }

            const stats = rowToMap(cells);
            const tpl = fs.readFileSync('team_result.mustache').toString();
            res.send(mustache.render(tpl, { country, ...stats }));
        });
});

// ----------------------------------------------------
// Start server
// ----------------------------------------------------
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
