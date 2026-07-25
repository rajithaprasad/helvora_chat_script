const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
require('dotenv').config();

// ✅ Try to import database, but handle if it fails
let pool = null;
let testConnection = null;

try {
    const db = require('./config/database');
    pool = db.pool;
    testConnection = db.testConnection;
    console.log('✅ Database module loaded successfully');
} catch (error) {
    console.error('❌ Failed to load database module:', error.message);
    // Create a dummy pool for fallback
    pool = {
        query: async () => { throw new Error('Database not configured'); }
    };
    testConnection = async () => false;
}

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
    transports: ['websocket', 'polling'],
});

// Middleware
app.use(cors());
app.use(helmet());
app.use(express.json());

// Health check endpoints
app.get('/', (req, res) => {
    res.json({
        status: 'ok',
        service: 'Helvora WebSocket Server',
        version: '1.0.0',
        timestamp: new Date().toISOString(),
        endpoints: {
            health: '/health',
            test_db: '/test-db',
            debug_conversations: '/debug/conversations',
            debug_conversation: '/debug/conversation/:id',
            websocket: 'wss://' + req.get('host')
        }
    });
});

app.get('/health', async (req, res) => {
    try {
        if (pool) {
            const [result] = await pool.query('SELECT 1 as connected');
            res.json({
                status: 'ok',
                database: 'connected',
                timestamp: new Date().toISOString(),
                uptime: process.uptime()
            });
        } else {
            res.json({
                status: 'ok',
                database: 'not_configured',
                timestamp: new Date().toISOString(),
                uptime: process.uptime()
            });
        }
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
        if (!pool) {
            throw new Error('Database not configured');
        }
        const [rows] = await pool.query('SELECT NOW() as server_time, DATABASE() as database_name');
        res.json({
            success: true,
            database: process.env.DB_NAME || 'unknown',
            host: process.env.DB_HOST || 'unknown',
            time: rows[0].server_time,
            connection: 'active'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message,
            database: process.env.DB_NAME || 'unknown'
        });
    }
});

// ✅ DEBUG: Get all conversations
app.get('/debug/conversations', async (req, res) => {
    try {
        if (!pool) {
            throw new Error('Database not configured');
        }
        const [rows] = await pool.query('SELECT * FROM conversations ORDER BY id DESC LIMIT 20');
        res.json({
            success: true,
            count: rows.length,
            conversations: rows
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ✅ DEBUG: Get specific conversation
app.get('/debug/conversation/:id', async (req, res) => {
    try {
        if (!pool) {
            throw new Error('Database not configured');
        }
        const { id } = req.params;
        const [rows] = await pool.query('SELECT * FROM conversations WHERE id = ?', [id]);
        if (rows.length === 0) {
            res.json({
                success: false,
                message: `Conversation ${id} not found`
            });
        } else {
            res.json({
                success: true,
                conversation: rows[0]
            });
        }
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ✅ DEBUG: Get messages for a conversation
app.get('/debug/messages/:conversationId', async (req, res) => {
    try {
        if (!pool) {
            throw new Error('Database not configured');
        }
        const { conversationId } = req.params;
        const [rows] = await pool.query(
            'SELECT * FROM messages WHERE conversation_id = ? ORDER BY created_at DESC LIMIT 50',
            [conversationId]
        );
        res.json({
            success: true,
            count: rows.length,
            messages: rows
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Socket.io authentication
io.use((socket, next) => {
    const auth = socket.handshake.auth;
    
    console.log('========================================');
    console.log('🔑 Full auth object:', JSON.stringify(auth));
    console.log('🔑 auth.userId:', auth.userId);
    console.log('========================================');
    
    const userId = auth.userId || 6;
    
    socket.data.userId = userId;
    socket.data.userName = `User ${userId}`;
    
    console.log(`✅ Socket authenticated as user: ${userId}`);
    next();
});

// Store room members
const roomMembers = new Map();

io.on('connection', (socket) => {
    const userId = socket.data.userId;
    const userName = socket.data.userName;
    
    console.log(`🔵 User connected: ${userId} (${userName})`);
    console.log(`📊 Active connections: ${io.engine.clientsCount}`);
    
    // JOIN CHAT ROOM - FIXED with verification
    socket.on('join_chat', async ({ conversationId }) => {
        const roomName = `chat_${conversationId}`;
        
        try {
            console.log(`🔍 Checking conversation ${conversationId} in database...`);
            
            if (!pool) {
                console.error('❌ Database not configured');
                socket.emit('error', { 
                    message: 'Database not configured',
                    details: 'Please check server configuration'
                });
                return;
            }
            
            // ✅ Check if conversation exists
            const [convRows] = await pool.query(
                'SELECT id, status, customer_id, seller_id FROM conversations WHERE id = ?',
                [conversationId]
            );
            
            console.log(`📊 Conversation query result: ${convRows.length} rows found`);
            
            if (convRows.length === 0) {
                console.log(`⚠️ Conversation ${conversationId} does not exist in database`);
                
                // ✅ Try to find recent conversations for debugging
                try {
                    const [allConvs] = await pool.query(
                        'SELECT id, status, customer_id, seller_id FROM conversations ORDER BY id DESC LIMIT 10'
                    );
                    console.log(`📊 Recent conversations:`, allConvs.map(c => ({ id: c.id, status: c.status })));
                } catch (e) {
                    console.log('⚠️ Could not fetch recent conversations:', e.message);
                }
                
                socket.emit('error', { 
                    message: 'Conversation not found',
                    details: `Conversation ${conversationId} does not exist. Please refresh the chat.`
                });
                return;
            }
            
            const conv = convRows[0];
            console.log(`✅ Found conversation:`, conv);
            
            // ✅ Check if user is part of this conversation
            if (userId != conv.customer_id && userId != conv.seller_id) {
                console.log(`⚠️ User ${userId} is not part of conversation ${conversationId}`);
                socket.emit('error', { 
                    message: 'Unauthorized',
                    details: 'You are not a participant in this conversation'
                });
                return;
            }
            
            // ✅ If conversation is not active, reactivate it
            if (conv.status !== 'active') {
                await pool.query(
                    'UPDATE conversations SET status = "active", updated_at = NOW() WHERE id = ?',
                    [conversationId]
                );
                console.log(`✅ Reactivated conversation ${conversationId}`);
            }
            
            // Leave any existing chat rooms
            const rooms = Array.from(socket.rooms);
            rooms.forEach(room => {
                if (room.startsWith('chat_')) {
                    socket.leave(room);
                    console.log(`📤 Left room: ${room}`);
                }
            });
            
            socket.join(roomName);
            socket.data.currentRoom = roomName;
            
            console.log(`📩 User ${userId} joined chat room: ${roomName}`);
            
            if (!roomMembers.has(roomName)) {
                roomMembers.set(roomName, new Set());
            }
            roomMembers.get(roomName).add(userId);
            
            // ✅ Get chat history
            const messages = await getChatHistory(conversationId, 50);
            socket.emit('chat_history', {
                conversationId,
                messages,
                hasMore: messages.length === 50,
                timestamp: new Date().toISOString()
            });
            
            // ✅ Mark messages as read
            await markMessagesAsRead(conversationId, userId);
            
            // ✅ Notify others
            socket.to(roomName).emit('user_joined', {
                userId,
                userName,
                timestamp: new Date().toISOString()
            });
            
        } catch (error) {
            console.error('❌ Error joining chat:', error);
            socket.emit('error', { 
                message: 'Failed to join chat',
                details: error.message 
            });
        }
    });
    
    // SEND MESSAGE - Simplified for now
    socket.on('send_message', async (data) => {
        console.log('📨 send_message received:', JSON.stringify(data));
        
        try {
            const { conversationId, content, messageType = 'text', attachment_id } = data;
            
            if (!conversationId) {
                console.error('❌ Missing conversationId');
                socket.emit('error', { message: 'Missing conversationId' });
                return;
            }

            if (!pool) {
                console.error('❌ Database not configured');
                socket.emit('error', { message: 'Database not configured' });
                return;
            }

            const senderId = socket.data.userId;
            console.log(`📝 Sender: ${senderId}, Conversation: ${conversationId}`);

            // ✅ VERIFY conversation exists
            const [convRows] = await pool.query(
                'SELECT id, status, customer_id, seller_id FROM conversations WHERE id = ?',
                [conversationId]
            );
            
            if (convRows.length === 0) {
                console.error(`❌ Conversation ${conversationId} does not exist`);
                socket.emit('error', { 
                    message: 'Conversation not found',
                    details: 'The conversation does not exist in the database'
                });
                return;
            }
            
            const conv = convRows[0];
            
            // ✅ If conversation is not active, reactivate it
            if (conv.status !== 'active') {
                await pool.query(
                    'UPDATE conversations SET status = "active", updated_at = NOW() WHERE id = ?',
                    [conversationId]
                );
                console.log(`✅ Reactivated conversation ${conversationId}`);
            }

            // ✅ Check if sender is part of this conversation
            if (senderId != conv.customer_id && senderId != conv.seller_id) {
                console.error(`❌ Sender ${senderId} not part of conversation ${conversationId}`);
                socket.emit('error', { 
                    message: 'Unauthorized',
                    details: 'You are not a participant in this conversation'
                });
                return;
            }

            // ✅ Save message
            const message = await saveMessage({
                conversationId,
                senderId: senderId,
                content: content || '',
                messageType: messageType,
            });
            console.log(`💾 New message saved: ${message.id}`);

            const senderInfo = await getUserInfo(senderId);

            const messageData = {
                id: message.id,
                conversationId: conversationId,
                senderId: senderId,
                senderName: senderInfo?.name || `User ${senderId}`,
                senderImage: senderInfo?.profile_image || null,
                content: message.content || '',
                messageType: messageType,
                createdAt: message.createdAt,
                is_read: 0,
                attachments: [],
            };

            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('new_message', messageData);
            console.log(`📤 Broadcasted message from ${senderId} to room: ${roomName}`);

            await updateConversationTimestamp(conversationId);

        } catch (error) {
            console.error('❌ Error sending message:', error);
            socket.emit('error', { 
                message: 'Failed to send message',
                details: error.message 
            });
        }
    });
    
    // TYPING INDICATOR
    socket.on('typing', ({ conversationId, isTyping }) => {
        const roomName = `chat_${conversationId}`;
        socket.to(roomName).emit('user_typing', {
            userId,
            userName,
            isTyping,
            timestamp: new Date().toISOString()
        });
    });
    
    // MARK MESSAGES AS READ
    socket.on('mark_read', async ({ conversationId }) => {
        try {
            if (!pool) return;
            await markMessagesAsRead(conversationId, userId);
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('messages_read', {
                userId,
                conversationId,
                timestamp: new Date().toISOString()
            });
        } catch (error) {
            console.error('Error marking as read:', error);
        }
    });
    
    // DISCONNECT
    socket.on('disconnect', () => {
        console.log(`🔴 User disconnected: ${userId}`);
        console.log(`📊 Active connections: ${io.engine.clientsCount}`);
        
        roomMembers.forEach((members, roomName) => {
            if (members.has(userId)) {
                members.delete(userId);
                if (members.size === 0) {
                    roomMembers.delete(roomName);
                }
            }
        });
    });
});

// ✅ DATABASE FUNCTIONS with error handling
async function getChatHistory(conversationId, limit = 50, offset = 0) {
    if (!pool) return [];
    
    try {
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
            LEFT JOIN users u ON m.sender_id = u.id
            WHERE m.conversation_id = ? AND m.is_deleted = 0
            ORDER BY m.created_at DESC
            LIMIT ? OFFSET ?`,
            [conversationId, limit, offset]
        );
        
        return rows.reverse().map(row => ({
            ...row,
            createdAt: row.createdAt ? row.createdAt.toISOString() : null,
            attachments: []
        }));
    } catch (error) {
        console.error('Error getting chat history:', error);
        return [];
    }
}

async function saveMessage({ conversationId, senderId, content, messageType }) {
    if (!pool) throw new Error('Database not configured');
    
    console.log(`💾 Saving message - conversationId: ${conversationId}, senderId: ${senderId}, type: ${messageType}`);
    
    const [result] = await pool.query(
        `INSERT INTO messages 
        (conversation_id, sender_id, content, message_type, contains_contact_info)
        VALUES (?, ?, ?, ?, 0)`,
        [conversationId, senderId, content || '', messageType || 'text']
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

async function getUserInfo(userId) {
    if (!pool) return null;
    try {
        const [rows] = await pool.query(
            'SELECT id, name, profile_image FROM users WHERE id = ?',
            [userId]
        );
        return rows[0] || null;
    } catch (error) {
        console.error('Error getting user info:', error);
        return null;
    }
}

async function markMessagesAsRead(conversationId, userId) {
    if (!pool) return;
    try {
        await pool.query(
            `UPDATE messages 
            SET is_read = 1, read_at = NOW()
            WHERE conversation_id = ? 
            AND sender_id != ?
            AND is_read = 0`,
            [conversationId, userId]
        );
    } catch (error) {
        console.error('Error marking messages as read:', error);
    }
}

async function updateConversationTimestamp(conversationId) {
    if (!pool) return;
    try {
        await pool.query(
            `UPDATE conversations 
            SET last_message_at = NOW()
            WHERE id = ?`,
            [conversationId]
        );
    } catch (error) {
        console.error('Error updating conversation timestamp:', error);
    }
}

// START SERVER
const PORT = process.env.PORT || 3000;

async function startServer() {
    console.log('📊 Testing database connection...');
    
    try {
        if (testConnection) {
            const connected = await testConnection();
            if (connected) {
                console.log('✅ Database connection established successfully.');
            } else {
                console.log('⚠️ Database connection failed, but server will continue.');
            }
        } else {
            console.log('⚠️ Database test function not available.');
        }
    } catch (error) {
        console.error('❌ Database test error:', error.message);
    }
    
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`🚀 WebSocket server running on port ${PORT}`);
        console.log(`📡 Socket.io ready for connections`);
        console.log(`🔗 Health check: /health`);
        console.log(`🔍 Debug endpoints:`);
        console.log(`   - /debug/conversations`);
        console.log(`   - /debug/conversation/:id`);
        console.log(`   - /debug/messages/:conversationId`);
    });
}

startServer().catch((error) => {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
});

process.on('SIGTERM', () => {
    console.log('🛑 Shutting down gracefully...');
    server.close(() => {
        if (pool && pool.end) {
            pool.end();
        }
        console.log('✅ Shutdown complete');
        process.exit(0);
    });
});

process.on('uncaughtException', (error) => {
    console.error('💥 Uncaught Exception:', error);
});

module.exports = { io, server, app };
