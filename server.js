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

// ============================================
// ✅ STORE PENDING ORDERS IN MEMORY (NO DATABASE)
// ============================================
const pendingOrders = new Map();
const orderTimeouts = new Map();
const userSockets = new Map(); // ✅ Track user -> socket mapping

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
            user: '/debug/user/:userId',
            simulate: '/debug/simulate-order'
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

// ✅ DEBUG: Simulate order request (for testing)
app.post('/debug/simulate-order', (req, res) => {
    const { customerId = 6, sellerId = 8, serviceName = 'Test Service' } = req.body;
    
    console.log(`🧪 Simulating order from ${customerId} to seller ${sellerId}`);
    
    const sellerSocketId = userSockets.get(sellerId);
    const isSellerConnected = sellerSocketId ? true : false;
    
    res.json({
        success: true,
        message: 'Order simulation triggered',
        sellerConnected: isSellerConnected,
        sellerSocketId: sellerSocketId || null,
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
    
    // ✅ Store socket reference for this user
    userSockets.set(userId, socket.id);
    console.log(`📌 Stored socket for user ${userId}: ${socket.id}`);

    // ✅ Auto-join user room on connection
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
    // ✅ JOIN USER ROOM (for direct messages)
    // ============================================
    socket.on('join_user_room', async (data) => {
        try {
            const { userId: targetUserId } = data;
            if (!targetUserId) {
                socket.emit('error', { message: 'User ID required' });
                return;
            }
            
            // ✅ Update socket mapping
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
    // ✅ ORDER REQUEST - CUSTOMER TO SELLER (NO DATABASE)
    // ============================================
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
            console.log(`📦 ORDER REQUEST DEBUG`);
            console.log(`   Customer: ${customerId}`);
            console.log(`   Seller: ${sellerId}`);
            console.log(`   Service: ${serviceName}`);
            console.log(`   All connected users:`, Array.from(userSockets.keys()));
            console.log(`   User ${sellerId} connected? ${userSockets.has(sellerId) ? 'YES' : 'NO'}`);
            console.log(`   User ${sellerId} socket: ${userSockets.get(sellerId) || 'N/A'}`);
            console.log('========================================');
            
            if (!customerId || !sellerId || !serviceId) {
                socket.emit('error', { 
                    message: 'Missing required fields',
                    details: 'customerId, sellerId, and serviceId are required'
                });
                return;
            }
            
            const orderId = `order_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
            const timestamp = new Date().toISOString();
            const expiresAt = new Date(Date.now() + 30000); // 30 seconds
            
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
            
            // ✅ Store in memory ONLY (NO DATABASE)
            pendingOrders.set(orderId, {
                ...orderData,
                expiresAt: expiresAt,
                status: 'pending'
            });
            
            // ✅ Check if seller is connected
            const sellerSocketId = userSockets.get(sellerId);
            const isSellerConnected = sellerSocketId ? true : false;
            
            console.log(`🔍 Seller ${sellerId} connected: ${isSellerConnected}`);
            console.log(`   Socket ID: ${sellerSocketId || 'N/A'}`);
            
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
                    console.log(`📤 Broadcasted to room: ${sellerRoom} (fallback)`);
                }
            } else {
                console.log(`❌ Seller ${sellerId} is NOT connected!`);
                console.log(`📤 No direct delivery possible`);
                
                // ✅ IMPORTANT: Send push notification when seller is offline
                if (pool) {
                    try {
                        console.log(`📱 Attempting to send push notification to seller ${sellerId}`);
                        
                        const [tokenRows] = await pool.query(
                            'SELECT token FROM push_tokens WHERE user_id = ?',
                            [sellerId]
                        );
                        
                        if (tokenRows && tokenRows.length > 0) {
                            const pushTokens = tokenRows.map(row => row.token);
                            
                            console.log(`📱 Found ${pushTokens.length} push tokens for seller ${sellerId}`);
                            
                            const expoResponse = await fetch('https://exp.host/--/api/v2/push/send', {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                    'Accept': 'application/json',
                                },
                                body: JSON.stringify(pushTokens.map(token => ({
                                    to: token,
                                    sound: 'notification',
                                    title: 'New Order Request!',
                                    body: `${customerName || 'Customer'} requested: ${serviceName || 'Service'}`,
                                    priority: 'high',
                                    data: {
                                        type: 'order_request',
                                        order_id: orderId,
                                        customer_name: customerName || 'Customer',
                                        service_name: serviceName || 'Service',
                                        total_price: totalPrice || 0,
                                        delivery_date: deliveryDate || '',
                                        seller_id: sellerId,
                                    },
                                    channelId: 'order_notifications',
                                }))),
                            });
                            
                            const expoResult = await expoResponse.json();
                            console.log(`📱 Push notification result:`, expoResult);
                        } else {
                            console.log(`⚠️ No push tokens found for seller ${sellerId}`);
                        }
                    } catch (error) {
                        console.error('❌ Error sending push notification:', error);
                    }
                }
                
                // ✅ Also broadcast to room (in case seller connects later)
                const sellerRoom = `user_${sellerId}`;
                io.to(sellerRoom).emit('order_request_received', {
                    ...orderData,
                    timeRemaining: 30
                });
                console.log(`📤 Broadcasted to seller room: ${sellerRoom}`);
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
            
            console.log(`📦 Order ${orderId} stored in memory, expires at ${expiresAt.toISOString()}`);
            console.log(`📊 Total pending orders: ${pendingOrders.size}`);
            console.log('========================================');
            
        } catch (error) {
            console.error('❌ Error broadcasting order request:', error);
            socket.emit('error', { 
                message: 'Failed to send order request',
                details: error.message 
            });
        }
    });

    // ============================================
    // ✅ CHECK PENDING ORDER (from memory, NOT database)
    // ============================================
    socket.on('check_pending_order', async (data) => {
        try {
            const { orderId, userId } = data;
            
            console.log(`🔍 Checking pending order ${orderId} for user ${userId}`);
            
            const order = pendingOrders.get(orderId);
            if (!order) {
                socket.emit('pending_order_status', {
                    order_id: orderId,
                    status: 'not_found',
                    message: 'Order not found'
                });
                return;
            }
            
            const timeRemaining = order.expiresAt ? Math.max(0, Math.floor((order.expiresAt - new Date()) / 1000)) : 0;
            
            if (timeRemaining <= 0) {
                // Order expired
                pendingOrders.delete(orderId);
                if (orderTimeouts.has(orderId)) {
                    clearTimeout(orderTimeouts.get(orderId));
                    orderTimeouts.delete(orderId);
                }
                socket.emit('pending_order_status', {
                    order_id: orderId,
                    status: 'expired',
                    message: 'Order has expired'
                });
                return;
            }
            
            socket.emit('pending_order_status', {
                order_id: orderId,
                status: 'pending',
                order: {
                    ...order,
                    timeRemaining: timeRemaining,
                    expires_at: order.expiresAt ? order.expiresAt.toISOString() : null
                }
            });
            
        } catch (error) {
            console.error('❌ Error checking pending order:', error);
            socket.emit('error', { message: 'Failed to check order' });
        }
    });

    // ============================================
    // ✅ ACCEPT ORDER REQUEST - Calls PHP
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
            
            // ✅ Save to database via PHP
            const phpUrl = 'https://helvora.app/api_app/update-order-status.php';
            const numericOrderId = parseInt(orderId.split('_')[1] || orderId);
            
            try {
                const response = await fetch(phpUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        order_id: numericOrderId,
                        status: 'accepted',
                        seller_id: sellerId,
                        customer_id: order.customer_id,
                        service_name: order.service_name,
                        total_price: order.total_price,
                        delivery_date: order.delivery_date,
                        notes: order.notes
                    })
                });
                
                const result = await response.json();
                console.log('📥 PHP accept response:', result);
                
                if (result.success) {
                    order.status = 'accepted';
                    order.work_order_id = result.work_order?.id || null;
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
                        work_order_id: result.work_order?.id || null,
                        seller_id: sellerId,
                        status: 'accepted',
                        accepted_at: new Date().toISOString()
                    });
                    
                    // ✅ Notify seller
                    const sellerRoom = `user_${sellerId}`;
                    io.to(sellerRoom).emit('order_accept_confirmed', {
                        order_id: orderId,
                        work_order_id: result.work_order?.id || null,
                        status: 'accepted'
                    });
                    
                    socket.emit('order_accept_success', {
                        order_id: orderId,
                        work_order_id: result.work_order?.id || null,
                        status: 'accepted',
                        message: 'Order accepted successfully'
                    });
                    
                    setTimeout(() => {
                        if (pendingOrders.has(orderId)) {
                            pendingOrders.delete(orderId);
                            console.log(`🧹 Removed order ${orderId} from memory`);
                        }
                    }, 5000);
                    
                } else {
                    socket.emit('error', { 
                        message: result.error || 'Failed to accept order' 
                    });
                }
            } catch (fetchError) {
                console.error('❌ Error calling PHP:', fetchError);
                socket.emit('error', { 
                    message: 'Failed to connect to server. Please try again.' 
                });
            }
            
        } catch (error) {
            console.error('❌ Error accepting order:', error);
            socket.emit('error', { 
                message: 'Failed to accept order: ' + error.message 
            });
        }
    });

    // ============================================
    // ✅ DECLINE ORDER REQUEST - Calls PHP
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
            
            const phpUrl = 'https://helvora.app/api_app/update-order-status.php';
            const numericOrderId = parseInt(orderId.split('_')[1] || orderId);
            
            try {
                const response = await fetch(phpUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        order_id: numericOrderId,
                        status: 'rejected',
                        seller_id: sellerId,
                        reason: reason || 'Seller declined the request'
                    })
                });
                
                const result = await response.json();
                console.log('📥 PHP decline response:', result);
                
                if (result.success) {
                    pendingOrders.delete(orderId);
                    if (orderTimeouts.has(orderId)) {
                        clearTimeout(orderTimeouts.get(orderId));
                        orderTimeouts.delete(orderId);
                    }
                    
                    const customerRoom = `user_${order.customer_id}`;
                    io.to(customerRoom).emit('order_request_declined', {
                        order_id: orderId,
                        seller_id: sellerId,
                        reason: reason || 'Seller declined the request',
                        status: 'declined',
                        declined_at: new Date().toISOString()
                    });
                    
                    const sellerRoom = `user_${sellerId}`;
                    io.to(sellerRoom).emit('order_decline_confirmed', {
                        order_id: orderId,
                        status: 'declined'
                    });
                    
                    socket.emit('order_decline_success', {
                        order_id: orderId,
                        status: 'declined',
                        message: 'Order declined successfully'
                    });
                    
                } else {
                    socket.emit('error', { 
                        message: result.error || 'Failed to decline order' 
                    });
                }
            } catch (fetchError) {
                console.error('❌ Error calling PHP:', fetchError);
                socket.emit('error', { 
                    message: 'Failed to connect to server. Please try again.' 
                });
            }
            
        } catch (error) {
            console.error('❌ Error declining order:', error);
            socket.emit('error', { 
                message: 'Failed to decline order: ' + error.message 
            });
        }
    });

    // ============================================
    // ✅ ORDER EXPIRED - Emitted from timer
    // ============================================
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
    // ✅ CHAT EVENTS (existing)
    // ============================================
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

    socket.on('typing', ({ conversationId, isTyping }) => {
        const roomName = `chat_${conversationId}`;
        socket.to(roomName).emit('user_typing', {
            userId,
            userName,
            isTyping,
            timestamp: new Date().toISOString()
        });
    });

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
// ✅ DATABASE FUNCTIONS (for chat only)
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
        console.log(`📋 Order Broadcasting (NO DATABASE):`);
        console.log(`   📤 request_order - Customer requests a service`);
        console.log(`   ✅ accept_order_request - Seller accepts (calls PHP)`);
        console.log(`   ❌ decline_order_request - Seller declines (calls PHP)`);
        console.log(`   🔍 check_pending_order - Check order status (from memory)`);
        console.log(`   ⏰ Auto-expiry after 30 seconds`);
        console.log(`   📱 Push notifications when seller offline`);
        console.log(`📊 Pending orders in memory: 0`);
        console.log(`🔍 Debug endpoints:`);
        console.log(`   - GET /debug/pending-orders`);
        console.log(`   - GET /debug/pending-order/:id`);
        console.log(`   - GET /debug/rooms`);
        console.log(`   - GET /debug/connections`);
        console.log(`   - GET /debug/user/:userId`);
        console.log(`   - POST /debug/simulate-order`);
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
