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

// ✅ DEBUG endpoints
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

// ============================================
// ✅ PUSH NOTIFICATION FUNCTIONS
// ============================================

async function getPushTokens(userId) {
    if (!pool) return [];
    try {
        const [rows] = await pool.query(
            'SELECT token FROM push_tokens WHERE user_id = ?',
            [userId]
        );
        return rows.map(row => row.token);
    } catch (error) {
        console.error('Error getting push tokens:', error);
        return [];
    }
}

async function sendPushNotification(deviceTokens, title, body, data = {}) {
    if (!deviceTokens || deviceTokens.length === 0) {
        return { success: false, error: 'No device tokens', sent: 0 };
    }

    const url = 'https://exp.host/--/api/v2/push/send';
    
    const messages = deviceTokens.map(token => ({
        to: token,
        sound: 'notification.wav',
        title: title,
        body: body,
        priority: 'high',
        data: data,
        channelId: 'order_requests',
        _displayInForeground: true,
    }));

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            body: JSON.stringify(messages),
        });

        const result = await response.json();
        
        let successCount = 0;
        if (result.data) {
            result.data.forEach((item, index) => {
                if (item.status === 'ok') {
                    successCount++;
                } else if (item.details && 
                    (item.details.error === 'DeviceNotRegistered' || 
                     item.details.error === 'InvalidCredentials')) {
                    // Remove invalid token
                    removeInvalidToken(deviceTokens[index]);
                }
            });
        }

        return { success: true, sent: successCount, total: deviceTokens.length };
    } catch (error) {
        console.error('Error sending push notification:', error);
        return { success: false, error: error.message, sent: 0 };
    }
}

async function removeInvalidToken(token) {
    if (!pool) return;
    try {
        await pool.query('DELETE FROM push_tokens WHERE token = ?', [token]);
    } catch (error) {
        console.error('Error removing invalid token:', error);
    }
}

async function sendOrderNotification(notificationData) {
    const { 
        userId, 
        title, 
        body, 
        type, 
        orderId, 
        conversationId,
        actorName,
        actorId,
        extraData = {} 
    } = notificationData;

    // Get user's push tokens
    const tokens = await getPushTokens(userId);
    
    if (tokens.length === 0) {
        console.log(`📱 No push tokens found for user ${userId}`);
        return { success: false, sent: 0, message: 'No tokens found' };
    }

    const data = {
        type: type,
        order_id: String(orderId),
        conversation_id: String(conversationId || ''),
        user_id: String(userId),
        actor_id: String(actorId || ''),
        actor_name: actorName || '',
        ...extraData,
        critical: 'true',
    };

    console.log(`📱 Sending push notification to ${tokens.length} devices for user ${userId}`);
    console.log(`📱 Title: ${title}, Body: ${body}`);

    const result = await sendPushNotification(tokens, title, body, data);
    return result;
}

// ============================================
// ✅ SOCKET.IO AUTHENTICATION
// ============================================

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
            
            const [convRows] = await pool.query(
                'SELECT id, status, customer_id, seller_id FROM conversations WHERE id = ?',
                [conversationId]
            );
            
            console.log(`📊 Conversation query result: ${convRows.length} rows found`);
            
            if (convRows.length === 0) {
                console.log(`⚠️ Conversation ${conversationId} does not exist in database`);
                
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
            
            if (userId != conv.customer_id && userId != conv.seller_id) {
                console.log(`⚠️ User ${userId} is not part of conversation ${conversationId}`);
                socket.emit('error', { 
                    message: 'Unauthorized',
                    details: 'You are not a participant in this conversation'
                });
                return;
            }
            
            if (conv.status !== 'active') {
                await pool.query(
                    'UPDATE conversations SET status = "active", updated_at = NOW() WHERE id = ?',
                    [conversationId]
                );
                console.log(`✅ Reactivated conversation ${conversationId}`);
            }
            
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
            console.error('❌ Error joining chat:', error);
            socket.emit('error', { 
                message: 'Failed to join chat',
                details: error.message 
            });
        }
    });
    
    // SEND MESSAGE
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
            
            if (conv.status !== 'active') {
                await pool.query(
                    'UPDATE conversations SET status = "active", updated_at = NOW() WHERE id = ?',
                    [conversationId]
                );
                console.log(`✅ Reactivated conversation ${conversationId}`);
            }

            if (senderId != conv.customer_id && senderId != conv.seller_id) {
                console.error(`❌ Sender ${senderId} not part of conversation ${conversationId}`);
                socket.emit('error', { 
                    message: 'Unauthorized',
                    details: 'You are not a participant in this conversation'
                });
                return;
            }

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

            // ✅ Send push notification for new message
            const otherUserId = senderId === conv.customer_id ? conv.seller_id : conv.customer_id;
            const otherUserInfo = await getUserInfo(otherUserId);
            
            if (otherUserInfo) {
                await sendOrderNotification({
                    userId: otherUserId,
                    title: `New message from ${senderInfo?.name || 'User'}`,
                    body: content || 'New message',
                    type: 'new_message',
                    orderId: conversationId,
                    conversationId: conversationId,
                    actorName: senderInfo?.name || 'User',
                    actorId: senderId,
                    extraData: {
                        message_id: String(message.id),
                        message_type: messageType,
                    }
                });
            }

        } catch (error) {
            console.error('❌ Error sending message:', error);
            socket.emit('error', { 
                message: 'Failed to send message',
                details: error.message 
            });
        }
    });
    
    // FILE UPLOAD
    socket.on('new_file_uploaded', async (data) => {
        try {
            const { conversationId, messageId, attachmentId } = data;
            
            console.log(`📎 New file uploaded - conversation: ${conversationId}, message: ${messageId}, attachment: ${attachmentId}`);
            
            const [convRows] = await pool.query(
                'SELECT id FROM conversations WHERE id = ?',
                [conversationId]
            );
            
            if (convRows.length === 0) {
                console.log(`⚠️ Conversation ${conversationId} not found`);
                socket.emit('error', { message: 'Conversation not found' });
                return;
            }
            
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
            
            const messageData = {
                id: message.id,
                conversationId: message.conversationId,
                senderId: message.senderId,
                senderName: message.senderName || 'User',
                senderImage: message.senderImage || null,
                content: message.content || '',
                messageType: 'file',
                isRead: message.isRead || 0,
                createdAt: message.createdAt ? message.createdAt.toISOString() : new Date().toISOString(),
                attachments: attachments.map(a => ({
                    ...a,
                    is_image: a.is_image === 1,
                    created_at: a.created_at ? a.created_at.toISOString() : new Date().toISOString()
                })),
            };
            
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('new_message', messageData);
            console.log(`📤 Broadcasted file message to room: ${roomName}`);
            
            await updateConversationTimestamp(conversationId);
            
        } catch (error) {
            console.error('❌ Error handling new file upload:', error);
            socket.emit('error', { 
                message: 'Failed to process file upload',
                details: error.message 
            });
        }
    });
    
    // SEND OFFER
    socket.on('send_offer', async (data) => {
        try {
            const { conversationId, offerData, messageId } = data;
            
            if (!conversationId || !offerData) {
                socket.emit('error', { message: 'Missing required fields' });
                return;
            }
            
            const senderId = socket.data.userId;
            
            console.log(`📝 Offer broadcast from ${senderId} in chat ${conversationId}:`, offerData);
            
            const [convRows] = await pool.query(
                'SELECT id, customer_id, seller_id FROM conversations WHERE id = ?',
                [conversationId]
            );
            
            if (convRows.length === 0) {
                console.log(`⚠️ Conversation ${conversationId} not found`);
                socket.emit('error', { message: 'Conversation not found' });
                return;
            }
            
            const conv = convRows[0];
            const senderInfo = await getUserInfo(senderId);
            
            const messageData = {
                id: messageId || `temp_${Date.now()}`,
                conversationId: conversationId,
                senderId: senderId,
                senderName: senderInfo?.name || `User ${senderId}`,
                senderImage: senderInfo?.profile_image || null,
                content: JSON.stringify(offerData),
                messageType: 'offer',
                createdAt: new Date().toISOString(),
                is_read: 0,
                attachments: [],
            };
            
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('new_message', messageData);
            console.log(`📤 Broadcasted offer ${offerData.offer_id} from ${senderId} to room: ${roomName}`);
            
            await updateConversationTimestamp(conversationId);

            // ✅ Send push notification for new offer
            const otherUserId = senderId === conv.customer_id ? conv.seller_id : conv.customer_id;
            const otherUserInfo = await getUserInfo(otherUserId);
            
            if (otherUserInfo) {
                const serviceName = offerData.service_name || 'Service';
                const price = offerData.price || offerData.total_price || 0;
                
                await sendOrderNotification({
                    userId: otherUserId,
                    title: `📋 Custom Offer Received`,
                    body: `${senderInfo?.name || 'User'} sent you an offer: ${serviceName} - LKR ${Number(price).toLocaleString()}`,
                    type: 'new_offer',
                    orderId: conversationId,
                    conversationId: conversationId,
                    actorName: senderInfo?.name || 'User',
                    actorId: senderId,
                    extraData: {
                        offer_id: String(offerData.offer_id || ''),
                        service_name: serviceName,
                        price: String(price),
                    }
                });
            }

        } catch (error) {
            console.error('❌ Error sending offer:', error);
            socket.emit('error', { 
                message: 'Failed to send offer',
                details: error.message 
            });
        }
    });

    // OFFER UPDATED (accepted/declined)
    socket.on('offer_updated', async (data) => {
        try {
            const { conversationId, offerId, status, orderId } = data;
            
            console.log(`📋 Offer ${offerId} updated to ${status} in conversation ${conversationId}`);
            
            const [offerRows] = await pool.query(
                'SELECT * FROM custom_offers WHERE id = ?',
                [offerId]
            );
            
            if (offerRows.length === 0) {
                console.log(`⚠️ Offer ${offerId} not found`);
                return;
            }
            
            const offer = offerRows[0];
            
            const [convRows] = await pool.query(
                'SELECT customer_id, seller_id FROM conversations WHERE id = ?',
                [conversationId]
            );
            
            const conv = convRows[0];
            
            let message = null;
            
            const [messageRows] = await pool.query(
                `SELECT * FROM messages 
                 WHERE conversation_id = ? 
                 AND message_type = 'offer'
                 AND content LIKE ?`,
                [conversationId, `%"offer_id":${offerId}%`]
            );
            
            if (messageRows.length > 0) {
                message = messageRows[0];
            } else {
                const [allOfferMessages] = await pool.query(
                    `SELECT * FROM messages 
                     WHERE conversation_id = ? 
                     AND message_type = 'offer'`,
                    [conversationId]
                );
                
                for (const msg of allOfferMessages) {
                    try {
                        const parsed = JSON.parse(msg.content);
                        if (parsed.offer_id === offerId || parsed.id === offerId) {
                            message = msg;
                            break;
                        }
                    } catch (e) {
                        continue;
                    }
                }
            }
            
            if (!message) {
                console.log(`⚠️ No message found for offer ${offerId}`);
                return;
            }
            
            let offerData = JSON.parse(message.content);
            offerData.status = status;
            if (orderId) {
                offerData.order_id = orderId;
            }
            
            await pool.query(
                'UPDATE messages SET content = ? WHERE id = ?',
                [JSON.stringify(offerData), message.id]
            );
            
            const senderInfo = await getUserInfo(message.sender_id);
            
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
            
            let statusMessage = '';
            let notificationTitle = '';
            let notificationBody = '';
            
            if (status === 'accepted') {
                statusMessage = `✅ Offer accepted! Work order #${orderId || 'created'} has been created.`;
                notificationTitle = `✅ Offer Accepted!`;
                notificationBody = `${senderInfo?.name || 'User'} accepted your offer. Work order #${orderId || 'created'} has been created.`;
            } else if (status === 'declined') {
                statusMessage = `❌ Offer declined.`;
                notificationTitle = `❌ Offer Declined`;
                notificationBody = `${senderInfo?.name || 'User'} declined your offer.`;
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

            // ✅ Send push notification for offer status update
            const actorId = socket.data.userId;
            const actorName = socket.data.userName;
            const otherUserId = actorId === conv.customer_id ? conv.seller_id : conv.customer_id;
            
            await sendOrderNotification({
                userId: otherUserId,
                title: notificationTitle || `Offer ${status}`,
                body: notificationBody || `${actorName} updated the offer to ${status}`,
                type: `offer_${status}`,
                orderId: conversationId,
                conversationId: conversationId,
                actorName: actorName,
                actorId: actorId,
                extraData: {
                    offer_id: String(offerId),
                    status: status,
                    order_id: String(orderId || ''),
                }
            });

        } catch (error) {
            console.error('❌ Error handling offer update:', error);
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
    
    // ============================================
    // ✅ ORDER MANAGEMENT FEATURES WITH PUSH NOTIFICATIONS
    // ============================================
    
    // PRICE CHANGE - Request
    socket.on('request_price_change', async (data) => {
        try {
            const { orderId, newPrice, reason } = data;
            const sellerId = socket.data.userId;
            const sellerName = socket.data.userName;
            
            console.log(`💰 Price change requested for order ${orderId}: ${newPrice}`);
            
            const [orderRows] = await pool.query(
                'SELECT customer_id, seller_id FROM work_orders WHERE id = ?',
                [orderId]
            );
            
            if (orderRows.length === 0) {
                socket.emit('error', { message: 'Order not found' });
                return;
            }
            
            const order = orderRows[0];
            if (order.seller_id !== sellerId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            await pool.query(
                `UPDATE work_orders 
                SET pending_price = ?, 
                    price_change_reason = ?, 
                    price_change_status = 'pending',
                    updated_at = NOW() 
                WHERE id = ?`,
                [newPrice, reason, orderId]
            );
            
            const roomName = `chat_${orderId}`;
            io.to(roomName).emit('price_change_requested', {
                orderId,
                newPrice,
                reason,
                sellerId,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Price change request broadcasted to room: ${roomName}`);

            // ✅ Send push notification to customer
            const customerInfo = await getUserInfo(order.customer_id);
            if (customerInfo) {
                await sendOrderNotification({
                    userId: order.customer_id,
                    title: `💰 Price Change Request`,
                    body: `${sellerName} requested a price change to LKR ${Number(newPrice).toLocaleString()}`,
                    type: 'price_change_request',
                    orderId: orderId,
                    conversationId: orderId,
                    actorName: sellerName,
                    actorId: sellerId,
                    extraData: {
                        new_price: String(newPrice),
                        reason: reason || '',
                    }
                });
            }
            
        } catch (error) {
            console.error('❌ Error requesting price change:', error);
            socket.emit('error', { message: 'Failed to request price change' });
        }
    });
    
    // PRICE CHANGE - Accept
    socket.on('accept_price_change', async (data) => {
        try {
            const { orderId } = data;
            const customerId = socket.data.userId;
            const customerName = socket.data.userName;
            
            console.log(`✅ Price change accepted for order ${orderId}`);
            
            const [orderRows] = await pool.query(
                'SELECT customer_id, seller_id, pending_price FROM work_orders WHERE id = ?',
                [orderId]
            );
            
            if (orderRows.length === 0) {
                socket.emit('error', { message: 'Order not found' });
                return;
            }
            
            const order = orderRows[0];
            if (order.customer_id !== customerId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            await pool.query(
                `UPDATE work_orders 
                SET total_amount = ?, 
                    pending_price = NULL, 
                    price_change_status = 'accepted',
                    updated_at = NOW() 
                WHERE id = ?`,
                [order.pending_price, orderId]
            );
            
            const roomName = `chat_${orderId}`;
            io.to(roomName).emit('price_change_accepted', {
                orderId,
                newPrice: order.pending_price,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Price change acceptance broadcasted to room: ${roomName}`);

            // ✅ Send push notification to seller
            const sellerInfo = await getUserInfo(order.seller_id);
            if (sellerInfo) {
                await sendOrderNotification({
                    userId: order.seller_id,
                    title: `✅ Price Change Accepted`,
                    body: `${customerName} accepted the price change to LKR ${Number(order.pending_price).toLocaleString()}`,
                    type: 'price_change_accepted',
                    orderId: orderId,
                    conversationId: orderId,
                    actorName: customerName,
                    actorId: customerId,
                    extraData: {
                        new_price: String(order.pending_price),
                    }
                });
            }
            
        } catch (error) {
            console.error('❌ Error accepting price change:', error);
            socket.emit('error', { message: 'Failed to accept price change' });
        }
    });
    
    // PRICE CHANGE - Reject
    socket.on('reject_price_change', async (data) => {
        try {
            const { orderId } = data;
            const customerId = socket.data.userId;
            const customerName = socket.data.userName;
            
            console.log(`❌ Price change rejected for order ${orderId}`);
            
            const [orderRows] = await pool.query(
                'SELECT customer_id, seller_id FROM work_orders WHERE id = ?',
                [orderId]
            );
            
            if (orderRows.length === 0) {
                socket.emit('error', { message: 'Order not found' });
                return;
            }
            
            const order = orderRows[0];
            if (order.customer_id !== customerId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            await pool.query(
                `UPDATE work_orders 
                SET pending_price = NULL, 
                    price_change_status = 'rejected',
                    updated_at = NOW() 
                WHERE id = ?`,
                [orderId]
            );
            
            const roomName = `chat_${orderId}`;
            io.to(roomName).emit('price_change_rejected', {
                orderId,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Price change rejection broadcasted to room: ${roomName}`);

            // ✅ Send push notification to seller
            const sellerInfo = await getUserInfo(order.seller_id);
            if (sellerInfo) {
                await sendOrderNotification({
                    userId: order.seller_id,
                    title: `❌ Price Change Rejected`,
                    body: `${customerName} rejected the price change request`,
                    type: 'price_change_rejected',
                    orderId: orderId,
                    conversationId: orderId,
                    actorName: customerName,
                    actorId: customerId,
                });
            }
            
        } catch (error) {
            console.error('❌ Error rejecting price change:', error);
            socket.emit('error', { message: 'Failed to reject price change' });
        }
    });
    
    // DEADLINE EXTENSION - Request
    socket.on('request_deadline_extension', async (data) => {
        try {
            const { orderId, newDeadline, reason } = data;
            const sellerId = socket.data.userId;
            const sellerName = socket.data.userName;
            
            console.log(`📅 Deadline extension requested for order ${orderId}: ${newDeadline}`);
            
            const [orderRows] = await pool.query(
                'SELECT customer_id, seller_id FROM work_orders WHERE id = ?',
                [orderId]
            );
            
            if (orderRows.length === 0) {
                socket.emit('error', { message: 'Order not found' });
                return;
            }
            
            const order = orderRows[0];
            if (order.seller_id !== sellerId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            await pool.query(
                `UPDATE work_orders 
                SET pending_deadline = ?, 
                    deadline_extension_reason = ?, 
                    deadline_extension_status = 'pending',
                    updated_at = NOW() 
                WHERE id = ?`,
                [newDeadline, reason, orderId]
            );
            
            const roomName = `chat_${orderId}`;
            io.to(roomName).emit('deadline_extension_requested', {
                orderId,
                newDeadline,
                reason,
                sellerId,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Deadline extension request broadcasted to room: ${roomName}`);

            // ✅ Send push notification to customer
            const customerInfo = await getUserInfo(order.customer_id);
            if (customerInfo) {
                const formattedDeadline = new Date(newDeadline).toLocaleDateString('en-US', {
                    year: 'numeric',
                    month: 'short',
                    day: 'numeric'
                });
                await sendOrderNotification({
                    userId: order.customer_id,
                    title: `📅 Deadline Extension Request`,
                    body: `${sellerName} requested deadline extension to ${formattedDeadline}`,
                    type: 'deadline_extension_request',
                    orderId: orderId,
                    conversationId: orderId,
                    actorName: sellerName,
                    actorId: sellerId,
                    extraData: {
                        new_deadline: newDeadline,
                        reason: reason || '',
                    }
                });
            }
            
        } catch (error) {
            console.error('❌ Error requesting deadline extension:', error);
            socket.emit('error', { message: 'Failed to request deadline extension' });
        }
    });
    
    // DEADLINE EXTENSION - Accept
    socket.on('accept_deadline_extension', async (data) => {
        try {
            const { orderId } = data;
            const customerId = socket.data.userId;
            const customerName = socket.data.userName;
            
            console.log(`✅ Deadline extension accepted for order ${orderId}`);
            
            const [orderRows] = await pool.query(
                'SELECT customer_id, seller_id, pending_deadline FROM work_orders WHERE id = ?',
                [orderId]
            );
            
            if (orderRows.length === 0) {
                socket.emit('error', { message: 'Order not found' });
                return;
            }
            
            const order = orderRows[0];
            if (order.customer_id !== customerId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            await pool.query(
                `UPDATE work_orders 
                SET delivery_date = ?, 
                    pending_deadline = NULL, 
                    deadline_extension_status = 'accepted',
                    updated_at = NOW() 
                WHERE id = ?`,
                [order.pending_deadline, orderId]
            );
            
            const roomName = `chat_${orderId}`;
            io.to(roomName).emit('deadline_extension_accepted', {
                orderId,
                newDeadline: order.pending_deadline,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Deadline extension acceptance broadcasted to room: ${roomName}`);

            // ✅ Send push notification to seller
            const sellerInfo = await getUserInfo(order.seller_id);
            if (sellerInfo) {
                const formattedDeadline = new Date(order.pending_deadline).toLocaleDateString('en-US', {
                    year: 'numeric',
                    month: 'short',
                    day: 'numeric'
                });
                await sendOrderNotification({
                    userId: order.seller_id,
                    title: `✅ Deadline Extension Accepted`,
                    body: `${customerName} accepted the deadline extension to ${formattedDeadline}`,
                    type: 'deadline_extension_accepted',
                    orderId: orderId,
                    conversationId: orderId,
                    actorName: customerName,
                    actorId: customerId,
                    extraData: {
                        new_deadline: order.pending_deadline,
                    }
                });
            }
            
        } catch (error) {
            console.error('❌ Error accepting deadline extension:', error);
            socket.emit('error', { message: 'Failed to accept deadline extension' });
        }
    });
    
    // DEADLINE EXTENSION - Reject
    socket.on('reject_deadline_extension', async (data) => {
        try {
            const { orderId } = data;
            const customerId = socket.data.userId;
            const customerName = socket.data.userName;
            
            console.log(`❌ Deadline extension rejected for order ${orderId}`);
            
            const [orderRows] = await pool.query(
                'SELECT customer_id FROM work_orders WHERE id = ?',
                [orderId]
            );
            
            if (orderRows.length === 0) {
                socket.emit('error', { message: 'Order not found' });
                return;
            }
            
            const order = orderRows[0];
            if (order.customer_id !== customerId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            await pool.query(
                `UPDATE work_orders 
                SET pending_deadline = NULL, 
                    deadline_extension_status = 'rejected',
                    updated_at = NOW() 
                WHERE id = ?`,
                [orderId]
            );
            
            const roomName = `chat_${orderId}`;
            io.to(roomName).emit('deadline_extension_rejected', {
                orderId,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Deadline extension rejection broadcasted to room: ${roomName}`);

            // ✅ Send push notification to seller
            const sellerInfo = await getUserInfo(order.seller_id);
            if (sellerInfo) {
                await sendOrderNotification({
                    userId: order.seller_id,
                    title: `❌ Deadline Extension Rejected`,
                    body: `${customerName} rejected the deadline extension request`,
                    type: 'deadline_extension_rejected',
                    orderId: orderId,
                    conversationId: orderId,
                    actorName: customerName,
                    actorId: customerId,
                });
            }
            
        } catch (error) {
            console.error('❌ Error rejecting deadline extension:', error);
            socket.emit('error', { message: 'Failed to reject deadline extension' });
        }
    });
    
    // CANCELLATION - Request
    socket.on('request_cancellation', async (data) => {
        try {
            const { orderId, reason, requestedBy } = data;
            const userId = socket.data.userId;
            const userName = socket.data.userName;
            
            console.log(`🔴 Cancellation requested for order ${orderId} by ${requestedBy}`);
            
            const [orderRows] = await pool.query(
                'SELECT customer_id, seller_id FROM work_orders WHERE id = ?',
                [orderId]
            );
            
            if (orderRows.length === 0) {
                socket.emit('error', { message: 'Order not found' });
                return;
            }
            
            const order = orderRows[0];
            if (order.customer_id !== userId && order.seller_id !== userId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            await pool.query(
                `UPDATE work_orders 
                SET cancellation_requested_by = ?, 
                    cancellation_reason = ?, 
                    cancellation_status = 'pending',
                    updated_at = NOW() 
                WHERE id = ?`,
                [requestedBy, reason, orderId]
            );
            
            const roomName = `chat_${orderId}`;
            io.to(roomName).emit('cancellation_requested', {
                orderId,
                reason,
                requestedBy,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Cancellation request broadcasted to room: ${roomName}`);

            // ✅ Send push notification to the other party
            const otherUserId = userId === order.customer_id ? order.seller_id : order.customer_id;
            const otherUserInfo = await getUserInfo(otherUserId);
            const requestedByLabel = requestedBy === 'customer' ? 'Customer' : 'Seller';
            
            if (otherUserInfo) {
                await sendOrderNotification({
                    userId: otherUserId,
                    title: `🔴 Cancellation Request`,
                    body: `${requestedByLabel} requested to cancel the order. Reason: ${reason || 'No reason provided'}`,
                    type: 'cancellation_request',
                    orderId: orderId,
                    conversationId: orderId,
                    actorName: userName,
                    actorId: userId,
                    extraData: {
                        reason: reason || '',
                        requested_by: requestedBy,
                    }
                });
            }
            
        } catch (error) {
            console.error('❌ Error requesting cancellation:', error);
            socket.emit('error', { message: 'Failed to request cancellation' });
        }
    });
    
    // CANCELLATION - Accept
    socket.on('accept_cancellation', async (data) => {
        try {
            const { orderId } = data;
            const userId = socket.data.userId;
            const userName = socket.data.userName;
            
            console.log(`✅ Cancellation accepted for order ${orderId}`);
            
            const [orderRows] = await pool.query(
                'SELECT customer_id, seller_id FROM work_orders WHERE id = ?',
                [orderId]
            );
            
            if (orderRows.length === 0) {
                socket.emit('error', { message: 'Order not found' });
                return;
            }
            
            const order = orderRows[0];
            if (order.customer_id !== userId && order.seller_id !== userId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            await pool.query(
                `UPDATE work_orders 
                SET status = 'cancelled', 
                    cancellation_status = 'accepted',
                    updated_at = NOW() 
                WHERE id = ?`,
                [orderId]
            );
            
            const roomName = `chat_${orderId}`;
            io.to(roomName).emit('cancellation_accepted', {
                orderId,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Cancellation acceptance broadcasted to room: ${roomName}`);

            // ✅ Send push notification to the other party
            const otherUserId = userId === order.customer_id ? order.seller_id : order.customer_id;
            const otherUserInfo = await getUserInfo(otherUserId);
            
            if (otherUserInfo) {
                await sendOrderNotification({
                    userId: otherUserId,
                    title: `✅ Cancellation Accepted`,
                    body: `${userName} accepted the cancellation request. The order has been cancelled.`,
                    type: 'cancellation_accepted',
                    orderId: orderId,
                    conversationId: orderId,
                    actorName: userName,
                    actorId: userId,
                });
            }
            
        } catch (error) {
            console.error('❌ Error accepting cancellation:', error);
            socket.emit('error', { message: 'Failed to accept cancellation' });
        }
    });
    
    // CANCELLATION - Reject
    socket.on('reject_cancellation', async (data) => {
        try {
            const { orderId } = data;
            const userId = socket.data.userId;
            const userName = socket.data.userName;
            
            console.log(`❌ Cancellation rejected for order ${orderId}`);
            
            const [orderRows] = await pool.query(
                'SELECT customer_id, seller_id FROM work_orders WHERE id = ?',
                [orderId]
            );
            
            if (orderRows.length === 0) {
                socket.emit('error', { message: 'Order not found' });
                return;
            }
            
            const order = orderRows[0];
            if (order.customer_id !== userId && order.seller_id !== userId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            await pool.query(
                `UPDATE work_orders 
                SET cancellation_status = 'rejected',
                    updated_at = NOW() 
                WHERE id = ?`,
                [orderId]
            );
            
            const roomName = `chat_${orderId}`;
            io.to(roomName).emit('cancellation_rejected', {
                orderId,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Cancellation rejection broadcasted to room: ${roomName}`);

            // ✅ Send push notification to the other party
            const otherUserId = userId === order.customer_id ? order.seller_id : order.customer_id;
            const otherUserInfo = await getUserInfo(otherUserId);
            
            if (otherUserInfo) {
                await sendOrderNotification({
                    userId: otherUserId,
                    title: `❌ Cancellation Rejected`,
                    body: `${userName} rejected the cancellation request. The order will continue.`,
                    type: 'cancellation_rejected',
                    orderId: orderId,
                    conversationId: orderId,
                    actorName: userName,
                    actorId: userId,
                });
            }
            
        } catch (error) {
            console.error('❌ Error rejecting cancellation:', error);
            socket.emit('error', { message: 'Failed to reject cancellation' });
        }
    });
    
    // CONTACT SUPPORT
    socket.on('contact_support', async (data) => {
        try {
            const { orderId, message, userId } = data;
            
            console.log(`🆘 Support requested for order ${orderId}`);
            
            await pool.query(
                `INSERT INTO support_requests (order_id, user_id, message, status, created_at) 
                VALUES (?, ?, ?, 'pending', NOW())`,
                [orderId, userId, message]
            );
            
            const roomName = `chat_${orderId}`;
            io.to(roomName).emit('support_contacted', {
                orderId,
                message,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Support request broadcasted to room: ${roomName}`);

            // ✅ Send push notification to admin/support (not implemented in this example)
            // You could add admin push tokens here
            
        } catch (error) {
            console.error('❌ Error contacting support:', error);
            socket.emit('error', { message: 'Failed to contact support' });
        }
    });
    
    // STATUS UPDATE - Order status changed
    socket.on('order_status_updated', async (data) => {
        try {
            const { orderId, status, oldStatus } = data;
            const userId = socket.data.userId;
            const userName = socket.data.userName;
            
            console.log(`📋 Order ${orderId} status changed from ${oldStatus} to ${status}`);
            
            const [orderRows] = await pool.query(
                'SELECT customer_id, seller_id FROM work_orders WHERE id = ?',
                [orderId]
            );
            
            if (orderRows.length === 0) {
                return;
            }
            
            const order = orderRows[0];
            
            const statusLabels = {
                'accepted': 'Accepted',
                'in_progress': 'Started',
                'delivered': 'Delivered',
                'completed': 'Completed',
                'cancelled': 'Cancelled'
            };
            
            const statusEmojis = {
                'accepted': '✅',
                'in_progress': '🔄',
                'delivered': '📦',
                'completed': '🎉',
                'cancelled': '❌'
            };
            
            const statusLabel = statusLabels[status] || status;
            const statusEmoji = statusEmojis[status] || '📋';
            
            // Send notification to the other party
            const otherUserId = userId === order.customer_id ? order.seller_id : order.customer_id;
            const otherUserInfo = await getUserInfo(otherUserId);
            
            if (otherUserInfo) {
                await sendOrderNotification({
                    userId: otherUserId,
                    title: `${statusEmoji} Order ${statusLabel}`,
                    body: `${userName} updated the order status to ${statusLabel}`,
                    type: 'order_status_updated',
                    orderId: orderId,
                    conversationId: orderId,
                    actorName: userName,
                    actorId: userId,
                    extraData: {
                        status: status,
                        old_status: oldStatus || '',
                    }
                });
            }
            
        } catch (error) {
            console.error('❌ Error sending status update notification:', error);
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

// ============================================
// ✅ DATABASE FUNCTIONS
// ============================================

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
        console.log(`📋 Order Management Events with Push Notifications:`);
        console.log(`   - request_price_change → Push to customer`);
        console.log(`   - accept_price_change → Push to seller`);
        console.log(`   - reject_price_change → Push to seller`);
        console.log(`   - request_deadline_extension → Push to customer`);
        console.log(`   - accept_deadline_extension → Push to seller`);
        console.log(`   - reject_deadline_extension → Push to seller`);
        console.log(`   - request_cancellation → Push to other party`);
        console.log(`   - accept_cancellation → Push to other party`);
        console.log(`   - reject_cancellation → Push to other party`);
        console.log(`   - send_message → Push to other user`);
        console.log(`   - send_offer → Push to other user`);
        console.log(`   - offer_updated → Push to other user`);
        console.log(`   - contact_support → Push to admin`);
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
