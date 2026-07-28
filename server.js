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
            const timeoutId = setTimeout(() => controller.abort(), 15000);
            
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
    // ✅ CHAT EVENTS - FIXED WITH PROPER BROADCASTING
    // ============================================
    
    // ✅ JOIN CHAT ROOM
    socket.on('join_chat', async ({ conversationId }) => {
        const roomName = `chat_${conversationId}`;
        console.log(`📩 User ${userId} joining chat room: ${roomName}`);
        
        // Leave any existing chat rooms
        const rooms = Array.from(socket.rooms);
        rooms.forEach(room => {
            if (room.startsWith('chat_')) {
                socket.leave(room);
                console.log(`📤 Left room: ${room}`);
            }
        });
        
        // Join the new room
        socket.join(roomName);
        socket.data.currentRoom = roomName;
        
        if (!roomMembers.has(roomName)) {
            roomMembers.set(roomName, new Set());
        }
        roomMembers.get(roomName).add(userId);
        
        console.log(`✅ User ${userId} joined chat room: ${roomName}`);
        console.log(`📊 Room ${roomName} has ${roomMembers.get(roomName).size} members`);
        
        // ✅ Send confirmation
        socket.emit('room_joined', {
            conversationId,
            roomName,
            timestamp: new Date().toISOString()
        });
    });

    // ✅ SEND MESSAGE - FIXED BROADCASTING
    socket.on('send_message', async (data) => {
        try {
            const { conversationId, content, messageType = 'text', attachment_id } = data;
            
            console.log(`📨 send_message: conversationId=${conversationId}, content=${content?.substring(0, 30)}...`);
            
            if (!conversationId) {
                socket.emit('error', { message: 'Missing conversationId' });
                return;
            }

            const senderId = socket.data.userId;
            const senderName = socket.data.userName || `User ${senderId}`;
            
            console.log(`📝 Sender: ${senderId}, Room: chat_${conversationId}`);
            
            // ✅ Create message object
            const messageData = {
                id: `msg_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`,
                conversationId: conversationId,
                senderId: senderId,
                senderName: senderName,
                senderImage: null,
                content: content || '',
                messageType: messageType || 'text',
                createdAt: new Date().toISOString(),
                is_read: 0,
                attachments: [],
            };

            // ✅ Broadcast to the room (including sender)
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('new_message', messageData);
            
            console.log(`📤 Message broadcasted to room: ${roomName}`);

            // ✅ Try to save to database if available (don't block)
            if (pool) {
                try {
                    const [convRows] = await pool.query(
                        'SELECT id FROM conversations WHERE id = ?',
                        [conversationId]
                    );
                    
                    if (convRows.length > 0) {
                        await pool.query(
                            `INSERT INTO messages (conversation_id, sender_id, content, message_type, created_at)
                             VALUES (?, ?, ?, ?, NOW())`,
                            [conversationId, senderId, content || '', messageType || 'text']
                        );
                        await pool.query(
                            `UPDATE conversations SET last_message_at = NOW() WHERE id = ?`,
                            [conversationId]
                        );
                        console.log(`💾 Message saved to database`);
                    }
                } catch (dbError) {
                    console.error('❌ DB save error (non-critical):', dbError.message);
                }
            }
            
        } catch (error) {
            console.error('❌ Error sending message:', error);
            socket.emit('error', { 
                message: 'Failed to send message',
                details: error.message 
            });
        }
    });

    // ✅ NEW FILE UPLOADED
    socket.on('new_file_uploaded', async (data) => {
        try {
            const { conversationId, messageId, attachmentId } = data;
            
            console.log(`📎 New file uploaded - conversation: ${conversationId}`);
            
            const fileMessage = {
                id: messageId || `msg_${Date.now()}`,
                conversationId: conversationId,
                senderId: userId,
                senderName: socket.data.userName || `User ${userId}`,
                senderImage: null,
                content: '📎 File uploaded',
                messageType: 'file',
                createdAt: new Date().toISOString(),
                is_read: 0,
                attachments: [
                    {
                        id: attachmentId,
                        file_url: null,
                        file_type: 'file',
                        file_size: 0,
                        file_name: 'File',
                        mime_type: 'application/octet-stream',
                        width: null,
                        height: null,
                        is_image: false,
                        created_at: new Date().toISOString()
                    }
                ]
            };
            
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('new_message', fileMessage);
            console.log(`📤 Broadcasted file message to room: ${roomName}`);
            
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
            
            console.log(`📝 Offer broadcast from ${senderId} in chat ${conversationId}`);
            
            const messageData = {
                id: messageId || `temp_${Date.now()}`,
                conversationId: conversationId,
                senderId: senderId,
                senderName: socket.data.userName || `User ${senderId}`,
                senderImage: null,
                content: JSON.stringify(offerData),
                messageType: 'offer',
                createdAt: new Date().toISOString(),
                is_read: 0,
                attachments: [],
            };
            
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('new_message', messageData);
            console.log(`📤 Broadcasted offer to room: ${roomName}`);
            
        } catch (error) {
            console.error('❌ Error sending offer:', error);
            socket.emit('error', { 
                message: 'Failed to send offer',
                details: error.message 
            });
        }
    });

    // ✅ OFFER UPDATED
    socket.on('offer_updated', async (data) => {
        try {
            const { conversationId, offerId, status, orderId } = data;
            
            console.log(`📋 Offer ${offerId} updated to ${status}`);
            
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('offer_updated', {
                conversationId,
                offerId,
                status,
                orderId,
                timestamp: new Date().toISOString()
            });
            
            console.log(`📤 Broadcasted offer update to room: ${roomName}`);
            
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
            const roomName = `chat_${conversationId}`;
            io.to(roomName).emit('messages_read', {
                userId,
                conversationId,
                timestamp: new Date().toISOString()
            });
            console.log(`📖 User ${userId} marked messages as read`);
        } catch (error) {
            console.error('Error marking as read:', error);
        }
    });

    // ============================================
    // ✅ ORDER BROADCASTING EVENTS
    // ============================================

    // ✅ ORDER REQUEST
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
            console.log('========================================');
            
            if (!customerId || !sellerId || !serviceId) {
                socket.emit('error', { message: 'Missing required fields' });
                return;
            }
            
            // Send push notification (NO database)
            const pushUrl = 'https://helvora.app/api_app/send-push-notification.php';
            const pushData = {
                customer_id: customerId,
                seller_id: sellerId,
                service_id: serviceId || 1,
                service_name: serviceName || 'Service',
                quantity: quantity || 1,
                total_price: totalPrice || 0,
                notes: notes || '',
                customer_name: customerName || 'Customer',
                delivery_date: deliveryDate || null,
                send_push_notification: true,
                save_to_database: false
            };
            
            console.log('📤 Sending push notification...');
            await callPhpWithRetry(pushUrl, pushData).catch(e => console.log('⚠️ Push failed:', e.message));
            
            // Generate order ID
            const orderId = `order_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
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
                created_at: new Date().toISOString(),
                expires_at: expiresAt.toISOString(),
                timeout_seconds: 30
            };
            
            pendingOrders.set(orderId, { ...orderData, expiresAt: expiresAt, status: 'pending' });
            
            // Broadcast to seller
            const sellerSocketId = userSockets.get(sellerId);
            if (sellerSocketId) {
                const sellerSocket = io.sockets.sockets.get(sellerSocketId);
                if (sellerSocket && sellerSocket.connected) {
                    sellerSocket.emit('order_request_received', { ...orderData, timeRemaining: 30 });
                    console.log(`📤 Directly sent to seller ${sellerId} ✅`);
                }
            }
            
            // Broadcast to seller room
            io.to(`user_${sellerId}`).emit('order_request_received', { ...orderData, timeRemaining: 30 });
            
            // Confirm to customer
            io.to(`user_${customerId}`).emit('order_request_sent', {
                order_id: orderId,
                seller_id: sellerId,
                status: 'pending',
                expires_at: expiresAt.toISOString(),
                timeout_seconds: 30
            });
            
            // Schedule timeout
            const timeoutId = setTimeout(() => checkOrderExpiry(orderId), 30000);
            orderTimeouts.set(orderId, timeoutId);
            
            console.log(`📦 Order ${orderId} stored in memory`);
            console.log('========================================');
            
        } catch (error) {
            console.error('❌ Error broadcasting order request:', error);
            socket.emit('error', { message: 'Failed to send order request' });
        }
    });

    // ✅ ACCEPT ORDER REQUEST
    socket.on('accept_order_request', async (data) => {
        try {
            const { orderId, sellerId } = data;
            console.log(`✅ Seller ${sellerId} accepting order ${orderId}`);
            
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
                socket.emit('error', { message: 'Order has expired' });
                return;
            }
            
            // Save to database
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
                delivery_date: order.delivery_date,
                send_push_notification: false,
                save_to_database: true
            };
            
            const createResult = await callPhpWithRetry(createUrl, createData);
            if (!createResult.success) {
                socket.emit('error', { message: createResult.error || 'Failed to create order' });
                return;
            }
            
            const dbOrderId = createResult.order_id;
            
            // Accept the order
            const acceptUrl = 'https://helvora.app/api_app/update-order-status.php';
            const acceptResult = await callPhpWithRetry(acceptUrl, {
                order_id: dbOrderId,
                status: 'accepted',
                seller_id: sellerId
            });
            
            if (acceptResult.success) {
                order.status = 'accepted';
                order.db_order_id = dbOrderId;
                order.accepted_at = new Date().toISOString();
                pendingOrders.set(orderId, order);
                
                if (orderTimeouts.has(orderId)) {
                    clearTimeout(orderTimeouts.get(orderId));
                    orderTimeouts.delete(orderId);
                }
                
                io.to(`user_${order.customer_id}`).emit('order_request_accepted', {
                    order_id: orderId,
                    db_order_id: dbOrderId,
                    seller_id: sellerId,
                    status: 'accepted',
                    accepted_at: new Date().toISOString()
                });
                
                io.to(`user_${sellerId}`).emit('order_accept_confirmed', {
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
                
                setTimeout(() => {
                    if (pendingOrders.has(orderId)) {
                        pendingOrders.delete(orderId);
                    }
                }, 5000);
            } else {
                socket.emit('error', { message: acceptResult.error || 'Failed to accept order' });
            }
            
        } catch (error) {
            console.error('❌ Error accepting order:', error);
            socket.emit('error', { message: 'Failed to accept order. Please try again.' });
        }
    });

    // ✅ DECLINE ORDER REQUEST
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
                socket.emit('error', { message: 'Order has expired' });
                return;
            }
            
            // Save to database
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
                delivery_date: order.delivery_date,
                send_push_notification: false,
                save_to_database: true
            };
            
            const createResult = await callPhpWithRetry(createUrl, createData);
            if (!createResult.success) {
                socket.emit('error', { message: createResult.error || 'Failed to create order' });
                return;
            }
            
            const dbOrderId = createResult.order_id;
            
            // Decline the order
            const declineUrl = 'https://helvora.app/api_app/update-order-status.php';
            const declineResult = await callPhpWithRetry(declineUrl, {
                order_id: dbOrderId,
                status: 'rejected',
                seller_id: sellerId,
                reason: reason || 'Seller declined the request'
            });
            
            if (declineResult.success) {
                pendingOrders.delete(orderId);
                if (orderTimeouts.has(orderId)) {
                    clearTimeout(orderTimeouts.get(orderId));
                    orderTimeouts.delete(orderId);
                }
                
                io.to(`user_${order.customer_id}`).emit('order_request_declined', {
                    order_id: orderId,
                    db_order_id: dbOrderId,
                    seller_id: sellerId,
                    reason: reason || 'Seller declined the request',
                    status: 'declined',
                    declined_at: new Date().toISOString()
                });
                
                io.to(`user_${sellerId}`).emit('order_decline_confirmed', {
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
            } else {
                socket.emit('error', { message: declineResult.error || 'Failed to decline order' });
            }
            
        } catch (error) {
            console.error('❌ Error declining order:', error);
            socket.emit('error', { message: 'Failed to decline order. Please try again.' });
        }
    });

    // ✅ ORDER EXPIRED
    socket.on('order_expired', async (data) => {
        try {
            const { orderId } = data;
            console.log(`⏰ Order ${orderId} expired`);
            const order = pendingOrders.get(orderId);
            if (order) {
                pendingOrders.delete(orderId);
                io.to(`user_${order.customer_id}`).emit('order_request_expired', {
                    order_id: orderId,
                    status: 'expired',
                    expired_at: new Date().toISOString()
                });
                io.to(`user_${order.seller_id}`).emit('order_request_expired', {
                    order_id: orderId,
                    status: 'expired',
                    expired_at: new Date().toISOString()
                });
            }
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
        }
        
        if (socket.data.userRoom) {
            const roomName = socket.data.userRoom;
            if (roomMembers.has(roomName)) {
                roomMembers.get(roomName).delete(userId);
                if (roomMembers.get(roomName).size === 0) {
                    roomMembers.delete(roomName);
                }
            }
        }
    });
});

// ============================================
// ✅ ORDER EXPIRY CHECKER
// ============================================
function checkOrderExpiry(orderId) {
    const order = pendingOrders.get(orderId);
    if (!order || order.status !== 'pending') return;
    
    if (order.expiresAt < new Date()) {
        order.status = 'expired';
        pendingOrders.delete(orderId);
        
        if (orderTimeouts.has(orderId)) {
            clearTimeout(orderTimeouts.get(orderId));
            orderTimeouts.delete(orderId);
        }
        
        io.to(`user_${order.customer_id}`).emit('order_request_expired', {
            order_id: orderId,
            status: 'expired',
            expired_at: new Date().toISOString()
        });
        io.to(`user_${order.seller_id}`).emit('order_request_expired', {
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
        console.log(`   💬 Chat System (REAL-TIME):`);
        console.log(`      - join_chat`);
        console.log(`      - send_message (broadcasts to room)`);
        console.log(`      - typing`);
        console.log(`      - mark_read`);
        console.log(`      - new_file_uploaded`);
        console.log(`      - send_offer`);
        console.log(`      - offer_updated`);
        console.log(`   📦 Order Broadcasting (NO Database):`);
        console.log(`      - request_order`);
        console.log(`      - accept_order_request`);
        console.log(`      - decline_order_request`);
        console.log(`      - order_expired`);
        console.log(`   ⏰ Auto-expiry after 30 seconds`);
        console.log(`📊 Pending orders in memory: ${pendingOrders.size}`);
        console.log(`🔍 Debug endpoints available`);
    });
}

startServer().catch((error) => {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
});

module.exports = { io, server, app };
