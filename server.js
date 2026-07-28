// lib/chat/socket.ts
import { getAuthToken } from '@/lib/api';
import io, { Socket } from 'socket.io-client';

const WS_URL = 'https://mynode-savj.onrender.com';

export interface ChatServiceType {
  socket: Socket | null;
  connect: (token?: string, userId?: number) => Socket;
  disconnect: () => void;
  isConnected: () => boolean;
  getSocket: () => Socket | null;
  joinChat: (conversationId: string) => void;
  sendMessage: (data: any) => void;
  markRead: (conversationId: string) => void;
  sendTyping: (conversationId: string, isTyping: boolean) => void;
}

// ✅ Singleton instance at module level
let globalSocketInstance: Socket | null = null;
let globalIsConnecting = false;
let globalUserId: number | null = null;
let globalConversationId: string | null = null;
let globalReconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 5;

class ChatService implements ChatServiceType {
  socket: Socket | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private isConnecting = false;
  private userId: number | null = null;
  private conversationId: string | null = null;

  connect(token?: string, userId?: number): Socket {
    // ✅ Use global singleton if it exists and is connected
    if (globalSocketInstance && globalSocketInstance.connected) {
      console.log('✅ Using existing global socket connection');
      this.socket = globalSocketInstance;
      this.userId = globalUserId;
      this.conversationId = globalConversationId;
      return this.socket;
    }

    // ✅ Prevent multiple connection attempts
    if (globalIsConnecting) {
      console.log('⏳ Global connection already in progress...');
      if (globalSocketInstance) {
        this.socket = globalSocketInstance;
        return this.socket;
      }
      throw new Error('Connection already in progress');
    }

    // ✅ If we have a disconnected global socket, try to reconnect
    if (globalSocketInstance && !globalSocketInstance.connected) {
      console.log('🔄 Global socket exists but disconnected, attempting reconnect...');
      globalIsConnecting = true;
      globalSocketInstance.connect();
      this.socket = globalSocketInstance;
      this.userId = globalUserId;
      this.conversationId = globalConversationId;
      return this.socket;
    }

    // ✅ Check if local socket exists and is connected
    if (this.socket && this.socket.connected) {
      console.log('⚠️ Local socket already connected, updating global');
      globalSocketInstance = this.socket;
      globalUserId = this.userId || userId || null;
      globalConversationId = this.conversationId;
      return this.socket;
    }

    // ✅ Prevent local duplicate connections
    if (this.isConnecting) {
      console.log('⏳ Local connection already in progress...');
      return this.socket!;
    }

    // ✅ Start new connection
    this.isConnecting = true;
    globalIsConnecting = true;
    this.userId = userId || null;
    globalUserId = this.userId;
    const authToken = token || getAuthToken() || '';
    
    console.log('🔌 Creating NEW WebSocket connection...', WS_URL);
    console.log('🔑 User ID for WebSocket:', userId);

    // Clean up old socket if exists
    if (this.socket) {
      this.socket.offAny();
      this.socket.disconnect();
      this.socket = null;
    }

    if (globalSocketInstance && globalSocketInstance !== this.socket) {
      globalSocketInstance.offAny();
      globalSocketInstance.disconnect();
      globalSocketInstance = null;
    }

    // Create new socket
    this.socket = io(WS_URL, {
      transports: ['websocket', 'polling'],
      path: '/socket.io',
      auth: {
        token: authToken,
        userId: userId,
      },
      timeout: 10000,
      reconnection: true,
      reconnectionAttempts: this.maxReconnectAttempts,
      reconnectionDelay: 2000,
      reconnectionDelayMax: 5000,
      forceNew: false,
    });

    // ✅ Store in global
    globalSocketInstance = this.socket;
    globalUserId = this.userId;

    this.socket.on('connect', () => {
      console.log('✅ WebSocket connected! ID:', this.socket?.id);
      console.log('📡 User ID:', this.userId);
      this.reconnectAttempts = 0;
      globalReconnectAttempts = 0;
      this.isConnecting = false;
      globalIsConnecting = false;
      
      // ✅ Auto-join user room for order notifications
      if (this.userId) {
        this.socket?.emit('join_user_room', { userId: this.userId });
        console.log(`👤 Auto-joined user room for ${this.userId}`);
      }
      
      // ✅ Auto-join conversation if we have one
      if (this.conversationId || globalConversationId) {
        const convId = this.conversationId || globalConversationId;
        console.log('📩 Auto-joining conversation:', convId);
        this.joinChat(convId!);
      }
    });

    this.socket.on('connect_error', (error) => {
      console.log('❌ WebSocket connection error:', error.message);
      this.reconnectAttempts++;
      globalReconnectAttempts++;
      this.isConnecting = false;
      globalIsConnecting = false;
      
      if (this.reconnectAttempts >= this.maxReconnectAttempts) {
        console.log('⚠️ Max reconnection attempts reached');
        if (this.socket && this.socket.io && this.socket.io.opts) {
          this.socket.io.opts.reconnection = false;
        }
      }
    });

    this.socket.on('disconnect', (reason) => {
      console.log('🔴 WebSocket disconnected:', reason);
      this.isConnecting = false;
      globalIsConnecting = false;
    });

    this.socket.on('reconnect', (attemptNumber) => {
      console.log('🔄 WebSocket reconnected after', attemptNumber, 'attempts');
      
      // ✅ Re-join user room on reconnect
      if (this.userId) {
        this.socket?.emit('join_user_room', { userId: this.userId });
        console.log(`👤 Re-joined user room for ${this.userId}`);
      }
      
      // ✅ Re-join conversation on reconnect
      const convId = this.conversationId || globalConversationId;
      if (convId) {
        console.log('📩 Re-joining conversation:', convId);
        this.joinChat(convId);
      }
    });

    // ✅ If already connected, join immediately
    if (this.socket.connected && this.userId) {
      this.socket.emit('join_user_room', { userId: this.userId });
      console.log(`👤 Immediate join user room for ${this.userId}`);
    }

    return this.socket;
  }

  disconnect(): void {
    console.log('🔌 Disconnecting WebSocket...');
    
    if (this.socket) {
      this.socket.offAny();
      if (this.socket.io && this.socket.io.opts) {
        this.socket.io.opts.reconnection = false;
      }
      this.socket.disconnect();
    }
    
    if (globalSocketInstance) {
      globalSocketInstance.offAny();
      if (globalSocketInstance.io && globalSocketInstance.io.opts) {
        globalSocketInstance.io.opts.reconnection = false;
      }
      globalSocketInstance.disconnect();
    }
    
    this.socket = null;
    globalSocketInstance = null;
    this.reconnectAttempts = 0;
    globalReconnectAttempts = 0;
    this.isConnecting = false;
    globalIsConnecting = false;
    this.userId = null;
    globalUserId = null;
    this.conversationId = null;
    globalConversationId = null;
  }

  isConnected(): boolean {
    return this.socket?.connected || false;
  }

  getSocket(): Socket | null {
    if (!this.socket && globalSocketInstance) {
      this.socket = globalSocketInstance;
      this.userId = globalUserId;
      this.conversationId = globalConversationId;
    }
    return this.socket || globalSocketInstance;
  }

  joinChat(conversationId: string): void {
    this.conversationId = conversationId;
    globalConversationId = conversationId;
    
    const socket = this.getSocket();
    if (socket && socket.connected) {
      console.log('📩 Joining chat room:', conversationId);
      socket.emit('join_chat', { conversationId });
    } else {
      console.log('⚠️ Cannot join chat - socket not connected. Will retry on connect.');
    }
  }

  sendMessage(data: any): void {
    const socket = this.getSocket();
    if (socket && socket.connected) {
      console.log('📤 Sending message via socket:', data);
      socket.emit('send_message', data);
    } else {
      console.log('⚠️ Cannot send message - socket not connected');
      if (this.userId || globalUserId) {
        this.connect(undefined, this.userId || globalUserId || undefined);
      }
    }
  }

  markRead(conversationId: string): void {
    const socket = this.getSocket();
    if (socket && socket.connected) {
      socket.emit('mark_read', { conversationId });
    }
  }

  sendTyping(conversationId: string, isTyping: boolean): void {
    const socket = this.getSocket();
    if (socket && socket.connected) {
      socket.emit('typing', { conversationId, isTyping });
    }
  }
}

// ✅ Export singleton instance
export const chatService = new ChatService();
