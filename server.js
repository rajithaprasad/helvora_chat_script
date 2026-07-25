// config/database.js
const mysql = require('mysql2/promise');
require('dotenv').config();

// Create connection pool
const pool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'testDB1',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    charset: 'utf8mb4'
});

// Test connection
async function testConnection() {
    try {
        const connection = await pool.getConnection();
        const [rows] = await connection.query('SELECT DATABASE() as db, NOW() as time');
        console.log(`✅ Connected to database: ${rows[0].db}`);
        console.log(`🕐 Server time: ${rows[0].time}`);
        
        // ✅ Check if conversations table exists
        const [tables] = await connection.query(
            "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'conversations'"
        );
        console.log(`📊 conversations table exists: ${tables[0].count > 0}`);
        
        // ✅ Count conversations
        if (tables[0].count > 0) {
            const [convCount] = await connection.query('SELECT COUNT(*) as count FROM conversations');
            console.log(`📊 Total conversations: ${convCount[0].count}`);
            
            // ✅ Show recent conversations
            const [recent] = await connection.query('SELECT id, status, customer_id, seller_id FROM conversations ORDER BY id DESC LIMIT 5');
            console.log(`📊 Recent conversations:`, recent);
        }
        
        connection.release();
        return true;
    } catch (error) {
        console.error('❌ Database connection failed:', error.message);
        return false;
    }
}

module.exports = { pool, testConnection };
