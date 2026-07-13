const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
require('dotenv').config();

const { pool, testConnection } = require('./config/database');
const { authenticateSocket } = require('./middleware/auth');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
        credentials: true,
    },
    pingTimeout: 60000,
    pingInterval: 25000,
    transports: ['websocket', 'polling'], // Allow both
});

// Handle WebSocket upgrade
server.on('upgrade', (request, socket, head) => {
    io.engine.handleUpgrade(request, socket, head, (ws) => {
        io.engine.emit('connection', ws, request);
    });
});

// Middleware
app.use(cors());
app.use(helmet());
app.use(express.json());

// ---------- HEALTH CHECK ENDPOINTS ----------

app.get('/', (req, res) => {
    res.json({
        status: 'ok',
        service: 'Helvora WebSocket Server',
        version: '1.0.0',
        timestamp: new Date().toISOString(),
        endpoints: {
            health: '/health',
            test_db: '/test-db',
            websocket: 'wss://' + req.get('host')
        }
    });
});

app.get('/health', async (req, res) => {
    try {
        const [result] = await pool.query('SELECT 1 as connected');
        res.json({
            status: 'ok',
            database: 'connected',
            timestamp: new Date().toISOString(),
            uptime: process.uptime()
        });
    } catch (error) {
        res.status(500).json({
            status: 'error',
            database: 'disconnected',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

app.get('/test-db', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT NOW() as server_time, DATABASE() as database_name');
        res.json({
            success: true,
            database: process.env.DB_NAME,
            host: process.env.DB_HOST,
            time: rows[0].server_time,
            connection: 'active'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message,
            database: process.env.DB_NAME
        });
    }
});

// Store active users
const activeUsers = new Map();
const userSockets = new Map();

// Authentication middleware for Socket.io
io.use(authenticateSocket);

// ---------- SOCKET.IO EVENTS ----------
io.on('connection', (socket) => {
    const userId = socket.data.userId;
    const userName = socket.data.userName;
    
    console.log(`🔵 User connected: ${userId} (${userName})`);
    console.log(`📊 Active connections: ${io.engine.clientsCount}`);
    
    activeUsers.set(userId, socket.id);
    userSockets.set(socket.id, userId);
    
    io.emit('user_online', { 
        userId, 
        userName, 
        status: 'online',
        timestamp: new Date().toISOString()
    });
    
    // ---------- JOIN CHAT ROOM ----------
    socket.on('join_chat', async ({ conversationId }) => {
        const roomName = `chat_${conversationId}`;
        
        const rooms = Array.from(socket.rooms);
        rooms.forEach(room => {
            if (room.startsWith('chat_')) {
                socket.leave(room);
            }
        });
        
        socket.join(roomName);
        socket.data.currentRoom = roomName;
        
        console.log(`📩 User ${userId} joined chat: ${conversationId}`);
        
        try {
            const messages = await getChatHistory(conversationId, 50);
            socket.emit('chat_history', {
                conversationId,
                messages,
                hasMore: messages.length === 50,
                timestamp: new Date().toISOString()
            });
            
            await markMessagesAsRead(conversationId, userId);
            
            socket.to(roomName).emit('user_joined', {
                userId,
                userName,
                timestamp: new Date().toISOString()
            });
            
            const unreadCount = await getUnreadCount(conversationId, userId);
            socket.emit('unread_count', { conversationId, count: unreadCount });
            
        } catch (error) {
            console.error('Error loading chat history:', error);
            socket.emit('error', { 
                message: 'Failed to load chat history',
                details: error.message 
            });
        }
    });
    
    // ---------- SEND MESSAGE ----------
    socket.on('send_message', async (data) => {
        try {
            const { conversationId, content, messageType = 'text' } = data;
            
            if (!content || !conversationId) {
                socket.emit('error', { message: 'Missing required fields' });
                return;
            }
            
            console.log(`📝 Message from ${userId} in chat ${conversationId}: ${content.substring(0, 50)}...`);
            
            const message = await saveMessage({
                conversationId,
                senderId: userId,
                content,
                messageType,
            });
            
            const messageData = {
                id: message.id,
                conversationId,
                senderId: userId,
                senderName: userName,
                senderImage: socket.data.userImage,
                content,
                messageType,
                createdAt: message.createdAt,
                is_read: 0,
            };
            
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('new_message', messageData);
            
            await updateConversationTimestamp(conversationId);
            
            const unreadCount = await getUnreadCount(conversationId, userId);
            socket.to(roomName).emit('unread_count', { 
                conversationId, 
                count: unreadCount 
            });
            
        } catch (error) {
            console.error('Error sending message:', error);
            socket.emit('error', { 
                message: 'Failed to send message',
                details: error.message 
            });
        }
    });
    
    // ---------- TYPING INDICATOR ----------
    socket.on('typing', ({ conversationId, isTyping }) => {
        const roomName = `chat_${conversationId}`;
        socket.to(roomName).emit('user_typing', {
            userId,
            userName,
            isTyping,
            timestamp: new Date().toISOString()
        });
    });
    
    // ---------- MARK MESSAGES AS READ ----------
    socket.on('mark_read', async ({ conversationId }) => {
        try {
            const updated = await markMessagesAsRead(conversationId, userId);
            
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('messages_read', {
                userId,
                conversationId,
                timestamp: new Date().toISOString()
            });
            
            const unreadCount = await getUnreadCount(conversationId, userId);
            socket.emit('unread_count', { conversationId, count: unreadCount });
            
        } catch (error) {
            console.error('Error marking as read:', error);
        }
    });
    
    // ---------- GET UNREAD COUNT ----------
    socket.on('get_unread', async ({ conversationId }) => {
        try {
            const count = await getUnreadCount(conversationId, userId);
            socket.emit('unread_count', { conversationId, count });
        } catch (error) {
            console.error('Error getting unread count:', error);
        }
    });
    
    // ---------- DISCONNECT ----------
    socket.on('disconnect', () => {
        console.log(`🔴 User disconnected: ${userId}`);
        console.log(`📊 Active connections: ${io.engine.clientsCount}`);
        
        activeUsers.delete(userId);
        userSockets.delete(socket.id);
        
        io.emit('user_offline', { 
            userId, 
            userName, 
            status: 'offline',
            timestamp: new Date().toISOString()
        });
    });
});

// ---------- DATABASE FUNCTIONS ----------

async function getChatHistory(conversationId, limit = 50, offset = 0) {
    const [rows] = await pool.query(
        `SELECT 
            m.id,
            m.conversation_id as conversationId,
            m.sender_id as senderId,
            m.content,
            m.message_type as messageType,
            m.is_read as isRead,
            m.created_at as createdAt,
            u.name as senderName,
            u.profile_image as senderImage
        FROM messages m
        JOIN users u ON m.sender_id = u.id
        WHERE m.conversation_id = ? AND m.is_deleted = 0
        ORDER BY m.created_at DESC
        LIMIT ? OFFSET ?`,
        [conversationId, limit, offset]
    );
    
    return rows.reverse().map(row => ({
        ...row,
        createdAt: row.createdAt ? row.createdAt.toISOString() : null
    }));
}

async function saveMessage({ conversationId, senderId, content, messageType }) {
    const [result] = await pool.query(
        `INSERT INTO messages 
        (conversation_id, sender_id, content, message_type, contains_contact_info)
        VALUES (?, ?, ?, ?, ?)`,
        [conversationId, senderId, content, messageType, 0]
    );
    
    const [rows] = await pool.query(
        `SELECT 
            id,
            conversation_id as conversationId,
            sender_id as senderId,
            content,
            message_type as messageType,
            is_read as isRead,
            created_at as createdAt
        FROM messages 
        WHERE id = ?`,
        [result.insertId]
    );
    
    return {
        ...rows[0],
        createdAt: rows[0].createdAt ? rows[0].createdAt.toISOString() : new Date().toISOString()
    };
}

async function markMessagesAsRead(conversationId, userId) {
    const [result] = await pool.query(
        `UPDATE messages 
        SET is_read = 1, read_at = NOW()
        WHERE conversation_id = ? 
        AND sender_id != ?
        AND is_read = 0`,
        [conversationId, userId]
    );
    return result.affectedRows;
}

async function updateConversationTimestamp(conversationId) {
    await pool.query(
        `UPDATE conversations 
        SET last_message_at = NOW()
        WHERE id = ?`,
        [conversationId]
    );
}

async function getUnreadCount(conversationId, userId) {
    const [rows] = await pool.query(
        `SELECT COUNT(*) as count 
        FROM messages 
        WHERE conversation_id = ? 
        AND sender_id != ?
        AND is_read = 0`,
        [conversationId, userId]
    );
    return rows[0].count;
}

// ---------- START SERVER ----------

const PORT = process.env.PORT || 3000;

async function startServer() {
    console.log('📊 Testing database connection...');
    const connected = await testConnection();
    
    if (!connected) {
        console.error('⚠️  Database connection failed.');
    } else {
        console.log('✅ Database connection established successfully.');
    }
    
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`🚀 WebSocket server running on port ${PORT}`);
        console.log(`📡 Socket.io ready for connections`);
        console.log(`🔗 Health check: https://${process.env.RENDER_EXTERNAL_HOSTNAME || 'localhost'}/health`);
        console.log(`🔗 WebSocket endpoint: wss://${process.env.RENDER_EXTERNAL_HOSTNAME || 'localhost'}`);
    });
}

startServer().catch(console.error);

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('🛑 Shutting down gracefully...');
    server.close(() => {
        pool.end();
        console.log('✅ Shutdown complete');
        process.exit(0);
    });
});

process.on('uncaughtException', (error) => {
    console.error('💥 Uncaught Exception:', error);
});

module.exports = { io, server, app };
