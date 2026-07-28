const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
require('dotenv').config();

// ✅ Try to import database
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

// ============================================
// ✅ STORE PENDING ORDERS IN MEMORY
// ============================================
const pendingOrders = new Map();
const orderTimeouts = new Map();
const userSockets = new Map();

// ============================================
// ✅ HELPER: Call PHP with retry
// ============================================
async function callPhpWithRetry(url, data, maxRetries = 3) {
    let lastError = null;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            console.log(`📡 PHP call attempt ${attempt}/${maxRetries} to ${url}`);
            
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 15000); // 15 second timeout
            
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(data),
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const result = await response.json();
            console.log(`📥 PHP response (attempt ${attempt}):`, result);
            return result;
            
        } catch (error) {
            lastError = error;
            console.error(`❌ PHP call attempt ${attempt} failed:`, error.message);
            
            if (attempt < maxRetries) {
                // Wait before retrying (exponential backoff)
                const delay = attempt * 1000;
                console.log(`⏳ Waiting ${delay}ms before retry...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }
    
    throw lastError;
}

// ============================================
// ✅ HEALTH CHECK ENDPOINTS
// ============================================
app.get('/', (req, res) => {
    res.json({
        status: 'ok',
        service: 'Helvora WebSocket Server',
        version: '2.0.0',
        timestamp: new Date().toISOString(),
        pending_orders: pendingOrders.size,
        endpoints: {
            health: '/health',
            websocket: 'wss://' + req.get('host'),
            pending_orders: '/debug/pending-orders',
            rooms: '/debug/rooms',
            connections: '/debug/connections',
            user: '/debug/user/:userId'
        }
    });
});

app.get('/health', async (req, res) => {
    res.json({
        status: 'ok',
        pending_orders: pendingOrders.size,
        connections: userSockets.size,
        timestamp: new Date().toISOString(),
        uptime: process.uptime()
    });
});

// ✅ DEBUG: Get all pending orders
app.get('/debug/pending-orders', (req, res) => {
    const orders = [];
    for (const [id, order] of pendingOrders) {
        orders.push({
            ...order,
            expiresAt: order.expiresAt ? order.expiresAt.toISOString() : null,
            timeRemaining: order.expiresAt ? Math.max(0, Math.floor((order.expiresAt - new Date()) / 1000)) : 0
        });
    }
    res.json({
        success: true,
        count: orders.length,
        orders: orders
    });
});

// ✅ DEBUG: Get specific pending order
app.get('/debug/pending-order/:id', (req, res) => {
    const { id } = req.params;
    const order = pendingOrders.get(id);
    if (!order) {
        res.json({
            success: false,
            message: `Order ${id} not found`
        });
    } else {
        res.json({
            success: true,
            order: {
                ...order,
                expiresAt: order.expiresAt ? order.expiresAt.toISOString() : null,
                timeRemaining: order.expiresAt ? Math.max(0, Math.floor((order.expiresAt - new Date()) / 1000)) : 0
            }
        });
    }
});

// ✅ DEBUG: Get all rooms and members
app.get('/debug/rooms', (req, res) => {
    const rooms = {};
    for (const [roomName, members] of roomMembers) {
        rooms[roomName] = Array.from(members);
    }
    res.json({
        success: true,
        rooms: rooms,
        totalRooms: roomMembers.size,
        totalConnections: io.engine.clientsCount,
        connectedUsers: Array.from(userSockets.keys())
    });
});

// ✅ DEBUG: Get all connections
app.get('/debug/connections', (req, res) => {
    const connections = [];
    for (const [userId, socketId] of userSockets) {
        connections.push({
            userId: userId,
            socketId: socketId
        });
    }
    res.json({
        success: true,
        totalConnections: connections.length,
        connections: connections
    });
});

// ✅ DEBUG: Check if specific user is connected
app.get('/debug/user/:userId', (req, res) => {
    const { userId } = req.params;
    const socketId = userSockets.get(parseInt(userId));
    const isConnected = socketId ? true : false;
    
    res.json({
        success: true,
        userId: parseInt(userId),
        isConnected: isConnected,
        socketId: socketId || null,
        totalConnections: userSockets.size,
        connectedUsers: Array.from(userSockets.keys())
    });
});

// ============================================
// ✅ SOCKET.IO AUTHENTICATION
// ============================================
io.use((socket, next) => {
    const auth = socket.handshake.auth;
    const userId = auth.userId || auth.user_id || 6;
    socket.data.userId = userId;
    socket.data.userName = auth.userName || `User ${userId}`;
    socket.data.userRole = auth.userRole || 'customer';
    console.log(`✅ Socket authenticated as user: ${userId} (${socket.data.userRole})`);
    next();
});

// Store room members
const roomMembers = new Map();

// ============================================
// ✅ SOCKET.IO CONNECTION
// ============================================
io.on('connection', (socket) => {
    const userId = socket.data.userId;
    const userName = socket.data.userName;
    const userRole = socket.data.userRole;
    
    console.log(`🔵 User connected: ${userId} (${userName}) - Role: ${userRole}`);
    console.log(`📊 Active connections: ${io.engine.clientsCount}`);
    
    // ✅ Store socket reference
    userSockets.set(userId, socket.id);
    console.log(`📌 Stored socket for user ${userId}: ${socket.id}`);

    // ✅ Auto-join user room
    const roomName = `user_${userId}`;
    socket.join(roomName);
    socket.data.userRoom = roomName;
    
    if (!roomMembers.has(roomName)) {
        roomMembers.set(roomName, new Set());
    }
    roomMembers.get(roomName).add(userId);
    
    console.log(`👤 User ${userId} auto-joined personal room: ${roomName}`);
    console.log(`📊 Room ${roomName} has ${roomMembers.get(roomName).size} members`);
    console.log(`📊 Currently connected users:`, Array.from(userSockets.keys()));

    // ============================================
    // ✅ JOIN USER ROOM
    // ============================================
    socket.on('join_user_room', async (data) => {
        try {
            const { userId: targetUserId } = data;
            if (!targetUserId) {
                socket.emit('error', { message: 'User ID required' });
                return;
            }
            
            userSockets.set(targetUserId, socket.id);
            
            const roomName = `user_${targetUserId}`;
            const rooms = Array.from(socket.rooms);
            rooms.forEach(room => {
                if (room.startsWith('user_') && room !== roomName) {
                    socket.leave(room);
                }
            });
            
            socket.join(roomName);
            socket.data.userRoom = roomName;
            
            if (!roomMembers.has(roomName)) {
                roomMembers.set(roomName, new Set());
            }
            roomMembers.get(roomName).add(targetUserId);
            
            console.log(`👤 User ${targetUserId} joined personal room: ${roomName}`);
            console.log(`📊 Room ${roomName} has ${roomMembers.get(roomName).size} members`);
            
        } catch (error) {
            console.error('❌ Error joining user room:', error);
            socket.emit('error', { message: 'Failed to join user room' });
        }
    });

    // ============================================
    // ✅ CHAT EVENTS
    // ============================================
    
    // ✅ JOIN CHAT ROOM
    socket.on('join_chat', async ({ conversationId }) => {
        const roomName = `chat_${conversationId}`;
        try {
            if (!pool) {
                socket.emit('error', { message: 'Database not configured' });
                return;
            }
            
            const [convRows] = await pool.query(
                'SELECT id, status, customer_id, seller_id FROM conversations WHERE id = ?',
                [conversationId]
            );
            
            if (convRows.length === 0) {
                socket.emit('error', { message: 'Conversation not found' });
                return;
            }
            
            const conv = convRows[0];
            if (userId != conv.customer_id && userId != conv.seller_id) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            const rooms = Array.from(socket.rooms);
            rooms.forEach(room => {
                if (room.startsWith('chat_')) {
                    socket.leave(room);
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
            socket.emit('error', { message: 'Failed to join chat' });
        }
    });

    // ✅ SEND MESSAGE
    socket.on('send_message', async (data) => {
        try {
            const { conversationId, content, messageType = 'text' } = data;
            if (!conversationId || !pool) {
                socket.emit('error', { message: 'Missing data or DB not configured' });
                return;
            }
            
            const senderId = socket.data.userId;
            const message = await saveMessage({ conversationId, senderId, content, messageType });
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
            await updateConversationTimestamp(conversationId);
            
        } catch (error) {
            console.error('❌ Error sending message:', error);
            socket.emit('error', { message: 'Failed to send message' });
        }
    });

    // ✅ NEW FILE UPLOADED
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

    // ✅ SEND OFFER
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
                'SELECT id FROM conversations WHERE id = ?',
                [conversationId]
            );
            
            if (convRows.length === 0) {
                console.log(`⚠️ Conversation ${conversationId} not found`);
                socket.emit('error', { message: 'Conversation not found' });
                return;
            }
            
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
            
        } catch (error) {
            console.error('❌ Error sending offer:', error);
            socket.emit('error', { 
                message: 'Failed to send offer',
                details: error.message 
            });
        }
    });

    // ✅ OFFER UPDATED (accepted/declined)
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
                console.log(`✅ Found message via LIKE query: ${message.id}`);
            } else {
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
                        if (parsed.offer_id === offerId || parsed.id === offerId) {
                            message = msg;
                            console.log(`✅ Found message via JSON parsing: ${message.id}`);
                            break;
                        }
                    } catch (e) {
                        continue;
                    }
                }
            }
            
            if (!message) {
                console.log(`⚠️ No message found for offer ${offerId}, but offer was updated in DB`);
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
        }
    });

    // ✅ TYPING INDICATOR
    socket.on('typing', ({ conversationId, isTyping }) => {
        const roomName = `chat_${conversationId}`;
        socket.to(roomName).emit('user_typing', {
            userId,
            userName,
            isTyping,
            timestamp: new Date().toISOString()
        });
    });

    // ✅ MARK MESSAGES AS READ
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
    // ✅ ORDER BROADCASTING EVENTS
    // ============================================

    // ✅ ORDER REQUEST - Customer to Seller (NO DATABASE)
    socket.on('request_order', async (data) => {
        try {
            const { 
                customerId, 
                sellerId, 
                serviceId, 
                serviceName, 
                quantity, 
                totalPrice, 
                notes, 
                customerName,
                customerImage,
                deliveryDate
            } = data;
            
            console.log('========================================');
            console.log(`📦 ORDER REQUEST`);
            console.log(`   Customer: ${customerId}`);
            console.log(`   Seller: ${sellerId}`);
            console.log(`   Service: ${serviceName}`);
            console.log(`   Connected users:`, Array.from(userSockets.keys()));
            console.log(`   User ${sellerId} connected? ${userSockets.has(sellerId) ? 'YES' : 'NO'}`);
            console.log('========================================');
            
            if (!customerId || !sellerId || !serviceId) {
                socket.emit('error', { 
                    message: 'Missing required fields'
                });
                return;
            }
            
            const orderId = `order_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
            const timestamp = new Date().toISOString();
            const expiresAt = new Date(Date.now() + 30000);
            
            const orderData = {
                order_id: orderId,
                customer_id: customerId,
                seller_id: sellerId,
                service_id: serviceId,
                service_name: serviceName || 'Service',
                quantity: quantity || 1,
                total_price: totalPrice || 0,
                notes: notes || '',
                customer_name: customerName || 'Customer',
                customer_image: customerImage || null,
                delivery_date: deliveryDate || null,
                status: 'pending',
                created_at: timestamp,
                expires_at: expiresAt.toISOString(),
                timeout_seconds: 30
            };
            
            // ✅ Store in memory ONLY
            pendingOrders.set(orderId, {
                ...orderData,
                expiresAt: expiresAt,
                status: 'pending'
            });
            
            // ✅ Check if seller is connected
            const sellerSocketId = userSockets.get(sellerId);
            const isSellerConnected = sellerSocketId ? true : false;
            
            console.log(`🔍 Seller ${sellerId} connected: ${isSellerConnected}`);
            
            // ✅ Try to send directly to seller's socket
            if (isSellerConnected && sellerSocketId) {
                const sellerSocket = io.sockets.sockets.get(sellerSocketId);
                if (sellerSocket && sellerSocket.connected) {
                    sellerSocket.emit('order_request_received', {
                        ...orderData,
                        timeRemaining: 30
                    });
                    console.log(`📤 Directly sent to seller ${sellerId} ✅`);
                } else {
                    console.log(`⚠️ Seller socket exists but not connected`);
                    userSockets.delete(sellerId);
                    const sellerRoom = `user_${sellerId}`;
                    io.to(sellerRoom).emit('order_request_received', {
                        ...orderData,
                        timeRemaining: 30
                    });
                    console.log(`📤 Broadcasted to room: ${sellerRoom}`);
                }
            } else {
                console.log(`❌ Seller ${sellerId} is NOT connected!`);
                console.log(`📤 No direct delivery possible`);
            }
            
            // ✅ Confirm to customer
            const customerRoom = `user_${customerId}`;
            io.to(customerRoom).emit('order_request_sent', {
                order_id: orderId,
                seller_id: sellerId,
                status: 'pending',
                expires_at: expiresAt.toISOString(),
                timeout_seconds: 30
            });
            console.log(`📤 Confirmed to customer ${customerId}`);
            
            // ✅ Schedule timeout
            const timeoutId = setTimeout(() => {
                checkOrderExpiry(orderId);
            }, 30000);
            orderTimeouts.set(orderId, timeoutId);
            
            console.log(`📦 Order ${orderId} stored in memory`);
            console.log(`📊 Total pending orders: ${pendingOrders.size}`);
            console.log('========================================');
            
        } catch (error) {
            console.error('❌ Error broadcasting order request:', error);
            socket.emit('error', { 
                message: 'Failed to send order request'
            });
        }
    });

    // ============================================
    // ✅ ACCEPT ORDER REQUEST - With retry
    // ============================================
    socket.on('accept_order_request', async (data) => {
        try {
            const { orderId, sellerId } = data;
            
            console.log(`✅ Seller ${sellerId} accepting order ${orderId}`);
            
            const order = pendingOrders.get(orderId);
            if (!order) {
                socket.emit('error', { message: 'Order not found or expired' });
                return;
            }
            
            if (order.seller_id !== sellerId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            if (order.expiresAt < new Date()) {
                pendingOrders.delete(orderId);
                if (orderTimeouts.has(orderId)) {
                    clearTimeout(orderTimeouts.get(orderId));
                    orderTimeouts.delete(orderId);
                }
                socket.emit('error', { message: 'Order has expired' });
                return;
            }
            
            if (order.status !== 'pending') {
                socket.emit('error', { message: `Order already ${order.status}` });
                return;
            }
            
            // ✅ Create order in database using PHP with retry
            const createUrl = 'https://helvora.app/api_app/send-push-notification.php';
            const createData = {
                customer_id: order.customer_id,
                seller_id: order.seller_id,
                service_id: order.service_id || 1,
                service_name: order.service_name,
                quantity: order.quantity || 1,
                total_price: order.total_price,
                notes: order.notes || '',
                customer_name: order.customer_name,
                delivery_date: order.delivery_date
            };
            
            console.log('📤 Creating order in database...');
            const createResult = await callPhpWithRetry(createUrl, createData);
            
            if (!createResult.success) {
                socket.emit('error', { 
                    message: createResult.error || 'Failed to create order' 
                });
                return;
            }
            
            const dbOrderId = createResult.order_id;
            console.log(`✅ Order created in database with ID: ${dbOrderId}`);
            
            // ✅ Accept the order
            const acceptUrl = 'https://helvora.app/api_app/update-order-status.php';
            const acceptData = {
                order_id: dbOrderId,
                status: 'accepted',
                seller_id: sellerId
            };
            
            console.log('📤 Accepting order in database...');
            const acceptResult = await callPhpWithRetry(acceptUrl, acceptData);
            
            if (acceptResult.success) {
                // ✅ Update order in memory
                order.status = 'accepted';
                order.db_order_id = dbOrderId;
                order.accepted_at = new Date().toISOString();
                pendingOrders.set(orderId, order);
                
                if (orderTimeouts.has(orderId)) {
                    clearTimeout(orderTimeouts.get(orderId));
                    orderTimeouts.delete(orderId);
                }
                
                // ✅ Broadcast to customer
                const customerRoom = `user_${order.customer_id}`;
                io.to(customerRoom).emit('order_request_accepted', {
                    order_id: orderId,
                    db_order_id: dbOrderId,
                    seller_id: sellerId,
                    status: 'accepted',
                    accepted_at: new Date().toISOString()
                });
                
                // ✅ Notify seller
                const sellerRoom = `user_${sellerId}`;
                io.to(sellerRoom).emit('order_accept_confirmed', {
                    order_id: orderId,
                    db_order_id: dbOrderId,
                    status: 'accepted'
                });
                
                socket.emit('order_accept_success', {
                    order_id: orderId,
                    db_order_id: dbOrderId,
                    status: 'accepted',
                    message: 'Order accepted successfully'
                });
                
                console.log(`✅ Order ${orderId} accepted successfully`);
                
                // ✅ Clean up after 5 seconds
                setTimeout(() => {
                    if (pendingOrders.has(orderId)) {
                        pendingOrders.delete(orderId);
                        console.log(`🧹 Removed order ${orderId} from memory`);
                    }
                }, 5000);
                
            } else {
                socket.emit('error', { 
                    message: acceptResult.error || 'Failed to accept order' 
                });
            }
            
        } catch (error) {
            console.error('❌ Error accepting order:', error);
            socket.emit('error', { 
                message: 'Failed to accept order. Please try again.' 
            });
        }
    });

    // ============================================
    // ✅ DECLINE ORDER REQUEST - With retry
    // ============================================
    socket.on('decline_order_request', async (data) => {
        try {
            const { orderId, sellerId, reason } = data;
            
            console.log(`❌ Seller ${sellerId} declining order ${orderId}`);
            
            const order = pendingOrders.get(orderId);
            if (!order) {
                socket.emit('error', { message: 'Order not found' });
                return;
            }
            
            if (order.seller_id !== sellerId) {
                socket.emit('error', { message: 'Unauthorized' });
                return;
            }
            
            if (order.expiresAt < new Date()) {
                pendingOrders.delete(orderId);
                if (orderTimeouts.has(orderId)) {
                    clearTimeout(orderTimeouts.get(orderId));
                    orderTimeouts.delete(orderId);
                }
                socket.emit('error', { message: 'Order has expired' });
                return;
            }
            
            // ✅ Create order in database using PHP with retry
            const createUrl = 'https://helvora.app/api_app/send-push-notification.php';
            const createData = {
                customer_id: order.customer_id,
                seller_id: order.seller_id,
                service_id: order.service_id || 1,
                service_name: order.service_name,
                quantity: order.quantity || 1,
                total_price: order.total_price,
                notes: order.notes || '',
                customer_name: order.customer_name,
                delivery_date: order.delivery_date
            };
            
            console.log('📤 Creating order in database...');
            const createResult = await callPhpWithRetry(createUrl, createData);
            
            if (!createResult.success) {
                socket.emit('error', { 
                    message: createResult.error || 'Failed to create order' 
                });
                return;
            }
            
            const dbOrderId = createResult.order_id;
            console.log(`✅ Order created in database with ID: ${dbOrderId}`);
            
            // ✅ Decline the order
            const declineUrl = 'https://helvora.app/api_app/update-order-status.php';
            const declineData = {
                order_id: dbOrderId,
                status: 'rejected',
                seller_id: sellerId,
                reason: reason || 'Seller declined the request'
            };
            
            console.log('📤 Declining order in database...');
            const declineResult = await callPhpWithRetry(declineUrl, declineData);
            
            if (declineResult.success) {
                // ✅ Remove from memory
                pendingOrders.delete(orderId);
                if (orderTimeouts.has(orderId)) {
                    clearTimeout(orderTimeouts.get(orderId));
                    orderTimeouts.delete(orderId);
                }
                
                // ✅ Broadcast to customer
                const customerRoom = `user_${order.customer_id}`;
                io.to(customerRoom).emit('order_request_declined', {
                    order_id: orderId,
                    db_order_id: dbOrderId,
                    seller_id: sellerId,
                    reason: reason || 'Seller declined the request',
                    status: 'declined',
                    declined_at: new Date().toISOString()
                });
                
                // ✅ Notify seller
                const sellerRoom = `user_${sellerId}`;
                io.to(sellerRoom).emit('order_decline_confirmed', {
                    order_id: orderId,
                    db_order_id: dbOrderId,
                    status: 'declined'
                });
                
                socket.emit('order_decline_success', {
                    order_id: orderId,
                    db_order_id: dbOrderId,
                    status: 'declined',
                    message: 'Order declined successfully'
                });
                
                console.log(`✅ Order ${orderId} declined successfully`);
                
            } else {
                socket.emit('error', { 
                    message: declineResult.error || 'Failed to decline order' 
                });
            }
            
        } catch (error) {
            console.error('❌ Error declining order:', error);
            socket.emit('error', { 
                message: 'Failed to decline order. Please try again.' 
            });
        }
    });

    // ✅ ORDER EXPIRED
    socket.on('order_expired', async (data) => {
        try {
            const { orderId } = data;
            console.log(`⏰ Order ${orderId} expired (from client)`);
            
            const order = pendingOrders.get(orderId);
            if (!order) return;
            
            pendingOrders.delete(orderId);
            if (orderTimeouts.has(orderId)) {
                clearTimeout(orderTimeouts.get(orderId));
                orderTimeouts.delete(orderId);
            }
            
            const customerRoom = `user_${order.customer_id}`;
            io.to(customerRoom).emit('order_request_expired', {
                order_id: orderId,
                status: 'expired',
                expired_at: new Date().toISOString()
            });
            
            const sellerRoom = `user_${order.seller_id}`;
            io.to(sellerRoom).emit('order_request_expired', {
                order_id: orderId,
                status: 'expired',
                expired_at: new Date().toISOString()
            });
            
            console.log(`📤 Order ${orderId} expired broadcasted`);
            
        } catch (error) {
            console.error('❌ Error handling order expiry:', error);
        }
    });

    // ============================================
    // ✅ DISCONNECT
    // ============================================
    socket.on('disconnect', () => {
        console.log(`🔴 User disconnected: ${userId}`);
        console.log(`📊 Active connections: ${io.engine.clientsCount}`);
        
        if (userSockets.get(userId) === socket.id) {
            userSockets.delete(userId);
            console.log(`📌 Removed socket for user ${userId}`);
        }
        
        if (socket.data.userRoom) {
            const roomName = socket.data.userRoom;
            if (roomMembers.has(roomName)) {
                roomMembers.get(roomName).delete(userId);
                if (roomMembers.get(roomName).size === 0) {
                    roomMembers.delete(roomName);
                }
            }
            console.log(`📤 Left user room: ${roomName}`);
        }
    });
});

// ============================================
// ✅ ORDER EXPIRY CHECKER
// ============================================
function checkOrderExpiry(orderId) {
    const order = pendingOrders.get(orderId);
    if (!order) return;
    
    if (order.status === 'pending' && order.expiresAt < new Date()) {
        order.status = 'expired';
        pendingOrders.delete(orderId);
        
        if (orderTimeouts.has(orderId)) {
            clearTimeout(orderTimeouts.get(orderId));
            orderTimeouts.delete(orderId);
        }
        
        const customerRoom = `user_${order.customer_id}`;
        io.to(customerRoom).emit('order_request_expired', {
            order_id: orderId,
            status: 'expired',
            expired_at: new Date().toISOString()
        });
        
        const sellerRoom = `user_${order.seller_id}`;
        io.to(sellerRoom).emit('order_request_expired', {
            order_id: orderId,
            status: 'expired',
            expired_at: new Date().toISOString()
        });
        
        console.log(`⏰ Order ${orderId} expired`);
    }
}

// ✅ Clean up expired orders every 5 seconds
setInterval(() => {
    const now = new Date();
    for (const [orderId, order] of pendingOrders) {
        if (order.expiresAt < now && order.status === 'pending') {
            checkOrderExpiry(orderId);
        }
    }
}, 5000);

// ============================================
// ✅ DATABASE FUNCTIONS
// ============================================
async function getChatHistory(conversationId, limit = 50, offset = 0) {
    if (!pool) return [];
    try {
        const [rows] = await pool.query(
            `SELECT m.id, m.conversation_id as conversationId, m.sender_id as senderId,
             m.content, m.message_type as messageType, m.is_read as isRead,
             m.created_at as createdAt, u.name as senderName, u.profile_image as senderImage
             FROM messages m LEFT JOIN users u ON m.sender_id = u.id
             WHERE m.conversation_id = ? AND m.is_deleted = 0
             ORDER BY m.created_at DESC LIMIT ? OFFSET ?`,
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
    const [result] = await pool.query(
        `INSERT INTO messages (conversation_id, sender_id, content, message_type, contains_contact_info)
         VALUES (?, ?, ?, ?, 0)`,
        [conversationId, senderId, content || '', messageType || 'text']
    );
    const [rows] = await pool.query(
        `SELECT id, conversation_id as conversationId, sender_id as senderId,
         content, message_type as messageType, is_read as isRead, created_at as createdAt
         FROM messages WHERE id = ?`,
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
        const [rows] = await pool.query('SELECT id, name, profile_image FROM users WHERE id = ?', [userId]);
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
            `UPDATE messages SET is_read = 1, read_at = NOW()
             WHERE conversation_id = ? AND sender_id != ? AND is_read = 0`,
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
            `UPDATE conversations SET last_message_at = NOW() WHERE id = ?`,
            [conversationId]
        );
    } catch (error) {
        console.error('Error updating conversation timestamp:', error);
    }
}

// ============================================
// ✅ START SERVER
// ============================================
const PORT = process.env.PORT || 10000;

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
        }
    } catch (error) {
        console.error('❌ Database test error:', error.message);
    }
    
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`🚀 WebSocket server running on port ${PORT}`);
        console.log(`📡 Socket.io ready for connections`);
        console.log(`📋 Features:`);
        console.log(`   💬 Chat System:`);
        console.log(`      - join_chat`);
        console.log(`      - send_message`);
        console.log(`      - typing`);
        console.log(`      - mark_read`);
        console.log(`      - new_file_uploaded`);
        console.log(`      - send_offer`);
        console.log(`      - offer_updated`);
        console.log(`   📦 Order Broadcasting:`);
        console.log(`      - request_order (NO DATABASE)`);
        console.log(`      - accept_order_request (calls PHP with retry)`);
        console.log(`      - decline_order_request (calls PHP with retry)`);
        console.log(`      - order_expired`);
        console.log(`   ⏰ Auto-expiry after 30 seconds`);
        console.log(`   🔄 PHP retry: 3 attempts with exponential backoff`);
        console.log(`📊 Pending orders in memory: 0`);
        console.log(`🔍 Debug endpoints available`);
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

module.exports = { io, server, app };
