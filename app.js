const express = require('express');
const redis = require('redis');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

const redisClient = redis.createClient({ url: process.env.REDIS_URL });
const pgPool = new Pool({ connectionString: process.env.DATABASE_URL });

redisClient.connect().catch(console.error);

// High-performance redirection endpoint
app.get('/:shortCode', async (req, res) => {
    const { shortCode } = req.params;

    try {
        // 1. Try fetching from high-speed cache
        const cachedUrl = await redisClient.get(shortCode);
        if (cachedUrl) {
            // Asynchronously log analytics to avoid blocking user response
            logAnalyticsWorker(shortCode, req.headers);
            return res.redirect(301, cachedUrl);
        }

        // 2. Fallback to Persistent Database
        const dbResult = await pgPool.query('SELECT long_url FROM urls WHERE short_code = $1', [shortCode]);
        if (dbResult.rows.length === 0) return res.status(404).send('URL Not Found');

        const longUrl = dbResult.rows[0].long_url;

        // 3. Hydrate Cache with a Time-To-Live (TTL)
        await redisClient.setEx(shortCode, 3600, longUrl);

        logAnalyticsWorker(shortCode, req.headers);
        return res.redirect(301, longUrl);
    } catch (err) {
        return res.status(500).json({ error: 'Internal Server Error', details: err.message });
    }
});

async function logAnalyticsWorker(code, headers) {
    // In production, push this to a message queue like RabbitMQ or Kafka
    const userAgent = headers['user-agent'];
    await pgPool.query('INSERT INTO analytics (short_code, user_agent, accessed_at) VALUES ($1, $2, NOW())', [code, userAgent]);
}

module.exports = app;
