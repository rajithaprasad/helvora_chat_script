const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
const jwt = require('jsonwebtoken');
require('dotenv').config();

const { pool, testConnection } = require('./config/database');

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
    
    // JOIN CHAT ROOM
    socket.on('join_chat', async ({ conversationId }) => {
        const roomName = `chat_${conversationId}`;
        
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
            
        } catch (error) {
            console.error('Error loading chat history:', error);
            socket.emit('error', { 
                message: 'Failed to load chat history',
                details: error.message 
            });
        }
    });
    
    // ✅ SEND MESSAGE - With attachment support
    socket.on('send_message', async (data) => {
        console.log('📨 send_message received:', JSON.stringify(data));
        
        try {
            const { conversationId, content, messageType = 'text', attachment_id } = data;
            
            if (!conversationId) {
                console.error('❌ Missing conversationId');
                socket.emit('error', { message: 'Missing conversationId' });
                return;
            }

            const senderId = socket.data.userId;
            console.log(`📝 Sender: ${senderId}, Conversation: ${conversationId}`);

            // ✅ If attachment_id is provided, fetch attachment details
            let attachment = null;
            if (attachment_id) {
                console.log(`📎 Fetching attachment: ${attachment_id}`);
                const [rows] = await pool.query(
                    'SELECT * FROM message_attachments WHERE id = ?',
                    [attachment_id]
                );
                if (rows.length > 0) {
                    attachment = rows[0];
                    console.log(`📎 Attachment found: ${attachment.file_name}, is_image: ${attachment.is_image}`);
                } else {
                    console.log(`⚠️ Attachment not found: ${attachment_id}`);
                }
            }

            // ✅ Determine content and message type
            let finalContent = content || '';
            let finalMessageType = messageType;

            if (attachment) {
                finalMessageType = 'file';
                
                // ✅ For images: show NO text, just the image
                if (attachment.is_image) {
                    finalContent = ''; // Empty content - just show the image
                } else {
                    // ✅ For files: show file name
                    finalContent = `📎 ${attachment.file_name}`;
                }
                console.log(`📝 Final content: "${finalContent}", type: ${finalMessageType}`);
            } else {
                console.log(`📝 Text message: "${content}"`);
            }

            // ✅ Save message
            const message = await saveMessage({
                conversationId,
                senderId: senderId,
                content: finalContent,
                messageType: finalMessageType,
            });
            console.log(`💾 Message saved: ${message.id}`);

            // ✅ Link attachment to message
            if (attachment && attachment_id) {
                await pool.query(
                    'UPDATE message_attachments SET message_id = ? WHERE id = ?',
                    [message.id, attachment_id]
                );
                
                const [updatedAttachment] = await pool.query(
                    'SELECT * FROM message_attachments WHERE id = ?',
                    [attachment_id]
                );
                if (updatedAttachment.length > 0) {
                    attachment = updatedAttachment[0];
                }
            }

            const senderInfo = await getUserInfo(senderId);

            const messageData = {
                id: message.id,
                conversationId,
                senderId: senderId,
                senderName: senderInfo?.name || `User ${senderId}`,
                senderImage: senderInfo?.profile_image || null,
                content: finalContent,
                messageType: finalMessageType,
                createdAt: message.createdAt,
                is_read: 0,
                attachments: attachment ? [attachment] : [],
            };

            console.log(`📤 Broadcasting message: ${JSON.stringify(messageData)}`);

            const roomName = `chat_${conversationId}`;
            
            const roomSockets = await io.in(roomName).fetchSockets();
            console.log(`📤 Room ${roomName} has ${roomSockets.length} sockets`);

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
    
    // SEND OFFER
    socket.on('send_offer', async (data) => {
        try {
            const { conversationId, offerData } = data;
            
            if (!conversationId || !offerData) {
                socket.emit('error', { message: 'Missing required fields' });
                return;
            }
            
            const senderId = socket.data.userId;
            
            console.log(`📝 Offer from ${senderId} in chat ${conversationId}: ${offerData.service_name}`);
            
            const message = await saveMessage({
                conversationId,
                senderId: senderId,
                content: JSON.stringify(offerData),
                messageType: 'offer',
            });
            
            const senderInfo = await getUserInfo(senderId);
            
            const messageData = {
                id: message.id,
                conversationId,
                senderId: senderId,
                senderName: senderInfo?.name || `User ${senderId}`,
                senderImage: senderInfo?.profile_image || null,
                content: JSON.stringify(offerData),
                messageType: 'offer',
                createdAt: message.createdAt,
                is_read: 0,
            };
            
            const roomName = `chat_${conversationId}`;
            
            const roomSockets = await io.in(roomName).fetchSockets();
            console.log(`📤 Room ${roomName} has ${roomSockets.length} sockets`);
            
            io.to(roomName).emit('new_message', messageData);
            
            await updateConversationTimestamp(conversationId);
            
        } catch (error) {
            console.error('Error sending offer:', error);
            socket.emit('error', { 
                message: 'Failed to send offer',
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

// DATABASE FUNCTIONS
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
        LEFT JOIN users u ON m.sender_id = u.id
        WHERE m.conversation_id = ? AND m.is_deleted = 0
        ORDER BY m.created_at DESC
        LIMIT ? OFFSET ?`,
        [conversationId, limit, offset]
    );
    
    const messages = [];
    for (const row of rows.reverse()) {
        const [attachments] = await pool.query(
            `SELECT 
                id,
                message_id,
                file_url,
                file_type,
                file_size,
                file_name,
                mime_type,
                width,
                height,
                is_image,
                created_at
            FROM message_attachments 
            WHERE message_id = ?`,
            [row.id]
        );
        
        messages.push({
            ...row,
            attachments: attachments.map(a => ({
                ...a,
                created_at: a.created_at.toISOString()
            })),
            createdAt: row.createdAt ? row.createdAt.toISOString() : null
        });
    }
    
    return messages;
}

async function saveMessage({ conversationId, senderId, content, messageType }) {
    console.log(`💾 Saving message - conversationId: ${conversationId}, senderId: ${senderId}, type: ${messageType}`);
    
    const [result] = await pool.query(
        `INSERT INTO messages 
        (conversation_id, sender_id, content, message_type, contains_contact_info)
        VALUES (?, ?, ?, ?, 0)`,
        [conversationId, senderId, content, messageType]
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
    const [rows] = await pool.query(
        'SELECT id, name, profile_image FROM users WHERE id = ?',
        [userId]
    );
    return rows[0] || null;
}

async function markMessagesAsRead(conversationId, userId) {
    await pool.query(
        `UPDATE messages 
        SET is_read = 1, read_at = NOW()
        WHERE conversation_id = ? 
        AND sender_id != ?
        AND is_read = 0`,
        [conversationId, userId]
    );
}

async function updateConversationTimestamp(conversationId) {
    await pool.query(
        `UPDATE conversations 
        SET last_message_at = NOW()
        WHERE id = ?`,
        [conversationId]
    );
}

// START SERVER
const PORT = process.env.PORT || 3000;

async function startServer() {
    console.log('📊 Testing database connection...');
    const connected = await testConnection();
    
    if (!connected) {
        console.error('⚠️ Database connection failed.');
    } else {
        console.log('✅ Database connection established successfully.');
    }
    
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`🚀 WebSocket server running on port ${PORT}`);
        console.log(`📡 Socket.io ready for connections`);
        console.log(`🔗 Health check: https://${process.env.RENDER_EXTERNAL_HOSTNAME || 'localhost'}/health`);
    });
}

startServer().catch(console.error);

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
