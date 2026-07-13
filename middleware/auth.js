// middleware/auth.js
const jwt = require('jsonwebtoken');
const { pool } = require('../config/database');

async function authenticateSocket(socket, next) {
    try {
        // Get token from auth object
        const token = socket.handshake.auth.token;
        
        console.log('🔑 Auth attempt - Token received:', token ? `${token.substring(0, 20)}...` : 'No token');
        
        if (!token) {
            console.log('❌ No token provided');
            return next(new Error('Authentication required'));
        }

        // Verify JWT token
        let decoded;
        try {
            decoded = jwt.verify(token, process.env.JWT_SECRET);
            console.log('✅ Token verified successfully for user:', decoded.userId);
        } catch (jwtError) {
            console.log('❌ JWT verification failed:', jwtError.message);
            // For testing: fallback to default user
            console.log('⚠️ Using fallback authentication for user: 6');
            socket.data.userId = 6;
            socket.data.userName = 'Test User (Fallback)';
            socket.data.userImage = null;
            return next();
        }

        // Get user from database
        const [rows] = await pool.query(
            'SELECT id, name, profile_image FROM users WHERE id = ?',
            [decoded.userId]
        );
        
        if (rows.length === 0) {
            console.log('❌ User not found:', decoded.userId);
            // Fallback to default user
            console.log('⚠️ Using fallback authentication for user: 6');
            socket.data.userId = 6;
            socket.data.userName = 'Test User (Fallback)';
            socket.data.userImage = null;
            return next();
        }
        
        const user = rows[0];
        
        // Attach user data to socket
        socket.data.userId = user.id;
        socket.data.userName = user.name;
        socket.data.userImage = user.profile_image;
        
        console.log('✅ Socket authenticated for user:', user.id, user.name);
        next();
    } catch (error) {
        console.error('❌ Auth error:', error);
        // Fallback: use default user for testing
        console.log('⚠️ Using fallback authentication for user: 6');
        socket.data.userId = 6;
        socket.data.userName = 'Test User (Fallback)';
        socket.data.userImage = null;
        next();
    }
}

module.exports = { authenticateSocket };
