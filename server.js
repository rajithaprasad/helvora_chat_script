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
    
    // ✅ SEND MESSAGE - With attachment support (for text and file messages)
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
            let existingMessageId = null;
            
            if (attachment_id) {
                console.log(`📎 Fetching attachment: ${attachment_id}`);
                const [rows] = await pool.query(
                    'SELECT * FROM message_attachments WHERE id = ?',
                    [attachment_id]
                );
                if (rows.length > 0) {
                    attachment = rows[0];
                    // ✅ Check if attachment already has a message_id
                    if (attachment.message_id) {
                        existingMessageId = attachment.message_id;
                        console.log(`✅ Attachment already linked to message: ${existingMessageId}`);
                    }
                    console.log(`📎 Attachment found: ${attachment.file_name}, is_image: ${attachment.is_image}`);
                } else {
                    console.log(`⚠️ Attachment not found: ${attachment_id}`);
                }
            }

            // ✅ Determine content and message type
            let finalContent = '';
            let finalMessageType = 'text';

            if (attachment) {
                finalMessageType = 'file';
                
                // ✅ For images: NO text at all
                if (attachment.is_image) {
                    finalContent = ''; // ✅ Completely empty for images
                    console.log('📸 Image message - no text');
                } else {
                    // ✅ For files: show file name
                    finalContent = `📎 ${attachment.file_name}`;
                    console.log(`📎 File message: ${finalContent}`);
                }
            } else if (content) {
                finalContent = content;
                finalMessageType = messageType;
            }

            let message;
            
            // ✅ If message already exists (from upload), use it
            if (existingMessageId) {
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
                    [existingMessageId]
                );
                if (rows.length > 0) {
                    message = {
                        id: rows[0].id,
                        conversationId: rows[0].conversationId,
                        senderId: rows[0].senderId,
                        content: rows[0].content,
                        messageType: 'file', // ✅ Force to 'file' since it has attachment
                        isRead: rows[0].isRead,
                        createdAt: rows[0].createdAt ? rows[0].createdAt.toISOString() : new Date().toISOString()
                    };
                    console.log(`✅ Using existing message: ${message.id}`);
                }
            }
            
            // ✅ If no existing message, create a new one
            if (!message) {
                message = await saveMessage({
                    conversationId,
                    senderId: senderId,
                    content: finalContent,
                    messageType: finalMessageType,
                });
                console.log(`💾 New message saved: ${message.id}`);

                // ✅ Link attachment to message (only if not already linked)
                if (attachment && attachment_id) {
                    await pool.query(
                        'UPDATE message_attachments SET message_id = ? WHERE id = ? AND message_id IS NULL',
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
            }

            const senderInfo = await getUserInfo(senderId);

            // ✅ Get attachments for the message
            let attachments = [];
            if (message.id) {
                const [attachmentRows] = await pool.query(
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
                    [message.id]
                );
                attachments = attachmentRows.map(a => ({
                    ...a,
                    is_image: a.is_image === 1,
                    created_at: a.created_at ? a.created_at.toISOString() : new Date().toISOString()
                }));
            }

            const messageData = {
                id: message.id,
                conversationId,
                senderId: senderId,
                senderName: senderInfo?.name || `User ${senderId}`,
                senderImage: senderInfo?.profile_image || null,
                content: message.content || finalContent,
                messageType: 'file', // ✅ Force to 'file' if has attachments
                createdAt: message.createdAt,
                is_read: 0,
                attachments: attachments,
            };

            console.log(`📤 Broadcasting message with ${messageData.attachments.length} attachments`);

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
    
    // ✅ NEW: Handle new file uploaded (from mobile app after PHP upload)
    socket.on('new_file_uploaded', async (data) => {
        try {
            const { conversationId, messageId, attachmentId } = data;
            
            console.log(`📎 New file uploaded - conversation: ${conversationId}, message: ${messageId}, attachment: ${attachmentId}`);
            
            // ✅ Get the message with attachment
            const [messageRows] = await pool.query(
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
                WHERE m.id = ?`,
                [messageId]
            );
            
            if (messageRows.length === 0) {
                console.log(`⚠️ Message ${messageId} not found`);
                socket.emit('error', { message: 'Message not found' });
                return;
            }
            
            const message = messageRows[0];
            
            // ✅ Get attachments
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
                [messageId]
            );
            
            // Format message data
            const messageData = {
                id: message.id,
                conversationId: message.conversationId,
                senderId: message.senderId,
                senderName: message.senderName || 'User',
                senderImage: message.senderImage || null,
                content: message.content || '',
                messageType: 'file', // ✅ Force to 'file' since it has attachment
                isRead: message.isRead || 0,
                createdAt: message.createdAt ? message.createdAt.toISOString() : new Date().toISOString(),
                attachments: attachments.map(a => ({
                    ...a,
                    is_image: a.is_image === 1,
                    created_at: a.created_at ? a.created_at.toISOString() : new Date().toISOString()
                })),
            };
            
            // ✅ Broadcast to room
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('new_message', messageData);
            console.log(`📤 Broadcasted file message to room: ${roomName}`);
            
            // ✅ Update conversation timestamp
            await updateConversationTimestamp(conversationId);
            
        } catch (error) {
            console.error('❌ Error handling new file upload:', error);
            socket.emit('error', { 
                message: 'Failed to process file upload',
                details: error.message 
            });
        }
    });
    
    // ✅ SEND OFFER
    socket.on('send_offer', async (data) => {
        try {
            const { conversationId, offerData } = data;
            
            if (!conversationId || !offerData) {
                socket.emit('error', { message: 'Missing required fields' });
                return;
            }
            
            const senderId = socket.data.userId;
            
            console.log(`📝 Offer from ${senderId} in chat ${conversationId}:`, offerData);
            
            // ✅ Save message with offer data as JSON string
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
                attachments: [],
            };
            
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('new_message', messageData);
            console.log(`📤 Broadcasted offer from ${senderId} to room: ${roomName}`);
            
            await updateConversationTimestamp(conversationId);
            
        } catch (error) {
            console.error('Error sending offer:', error);
            socket.emit('error', { 
                message: 'Failed to send offer',
                details: error.message 
            });
        }
    });

    // ✅ Handle offer updated (accepted/declined) - With graceful error handling
    socket.on('offer_updated', async (data) => {
        try {
            const { conversationId, offerId, status, orderId } = data;
            
            console.log(`📋 Offer ${offerId} updated to ${status} in conversation ${conversationId}`);
            
            // ✅ Get the updated offer from database
            const [offerRows] = await pool.query(
                'SELECT * FROM custom_offers WHERE id = ?',
                [offerId]
            );
            
            if (offerRows.length === 0) {
                console.log(`⚠️ Offer ${offerId} not found`);
                // ✅ Don't send error, just log it
                return;
            }
            
            const offer = offerRows[0];
            
            // ✅ Find the message that contains this offer - BETTER QUERY
            let message = null;
            
            // First try: Find by offer_id in content using LIKE
            const [messageRows] = await pool.query(
                `SELECT * FROM messages 
                 WHERE conversation_id = ? 
                 AND message_type = 'offer'
                 AND content LIKE ?`,
                [conversationId, `%"offer_id":${offerId}%`]
            );
            
            if (messageRows.length > 0) {
                message = messageRows[0];
                console.log(`✅ Found message via LIKE query: ${message.id}`);
            } else {
                // ✅ Second try: Get all offer messages and parse JSON
                console.log(`⚠️ No message found with LIKE query, trying JSON parsing...`);
                const [allOfferMessages] = await pool.query(
                    `SELECT * FROM messages 
                     WHERE conversation_id = ? 
                     AND message_type = 'offer'`,
                    [conversationId]
                );
                
                for (const msg of allOfferMessages) {
                    try {
                        const parsed = JSON.parse(msg.content);
                        // Check if this message contains the offer_id
                        if (parsed.offer_id === offerId || parsed.id === offerId) {
                            message = msg;
                            console.log(`✅ Found message via JSON parsing: ${message.id}`);
                            break;
                        }
                    } catch (e) {
                        // Skip invalid JSON
                        continue;
                    }
                }
            }
            
            if (!message) {
                console.log(`⚠️ No message found for offer ${offerId}, but offer was updated in DB`);
                // ✅ Don't send error, just log it and return
                return;
            }
            
            // ✅ Update the offer status in the message content
            let offerData = JSON.parse(message.content);
            offerData.status = status;
            if (orderId) {
                offerData.order_id = orderId;
            }
            
            // ✅ Update the message content
            await pool.query(
                'UPDATE messages SET content = ? WHERE id = ?',
                [JSON.stringify(offerData), message.id]
            );
            
            // ✅ Get sender info
            const senderInfo = await getUserInfo(message.sender_id);
            
            // ✅ Broadcast updated message to room
            const roomName = `chat_${conversationId}`;
            const messageData = {
                id: message.id,
                conversationId: conversationId,
                senderId: message.sender_id,
                senderName: senderInfo?.name || 'User',
                senderImage: senderInfo?.profile_image || null,
                content: JSON.stringify(offerData),
                messageType: 'offer',
                createdAt: message.created_at ? message.created_at.toISOString() : new Date().toISOString(),
                is_read: 1,
                attachments: [],
            };
            
            io.to(roomName).emit('offer_updated', messageData);
            console.log(`📤 Broadcasted offer update to room: ${roomName}`);
            
            // ✅ Also send a system message about the offer status change
            let statusMessage = '';
            if (status === 'accepted') {
                statusMessage = `✅ Offer accepted! Work order #${orderId || 'created'} has been created.`;
            } else if (status === 'declined') {
                statusMessage = `❌ Offer declined.`;
            }
            
            if (statusMessage) {
                const systemMessage = await saveMessage({
                    conversationId,
                    senderId: message.sender_id,
                    content: statusMessage,
                    messageType: 'text',
                });
                
                const systemMessageData = {
                    id: systemMessage.id,
                    conversationId: conversationId,
                    senderId: systemMessage.senderId,
                    senderName: 'System',
                    senderImage: null,
                    content: statusMessage,
                    messageType: 'text',
                    createdAt: systemMessage.createdAt,
                    is_read: 0,
                    attachments: [],
                };
                
                io.to(roomName).emit('new_message', systemMessageData);
                console.log(`📤 Broadcasted system message to room: ${roomName}`);
            }
            
        } catch (error) {
            console.error('❌ Error handling offer update:', error);
            // ✅ Don't emit error to client, just log it
            // The offer was already updated in the database
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

// ✅ DATABASE FUNCTIONS
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
        // ✅ Always fetch attachments for every message
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
        
        // ✅ If there are attachments, force messageType to 'file'
        let messageType = row.messageType;
        if (attachments.length > 0 && messageType !== 'offer') {
            messageType = 'file';
        }
        
        // ✅ If content is JSON and looks like an offer, set to 'offer'
        if (typeof row.content === 'string' && row.content.startsWith('{')) {
            try {
                const parsed = JSON.parse(row.content);
                if (parsed && (parsed.type === 'offer' || parsed.offer_id || parsed.service_name || parsed.seller_name)) {
                    messageType = 'offer';
                    console.log(`📋 Detected offer in message ${row.id} from DB`);
                }
            } catch (e) {
                // Not JSON, ignore
            }
        }
        
        messages.push({
            ...row,
            messageType: messageType,
            attachments: attachments.map(a => ({
                ...a,
                is_image: a.is_image === 1,
                created_at: a.created_at ? a.created_at.toISOString() : new Date().toISOString()
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
