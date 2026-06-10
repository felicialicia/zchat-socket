// This file is used by Railway (Node.js) and local dev (Bun)
const { createServer } = require('http')
const { Server } = require('socket.io')

const httpServer = createServer()
const io = new Server(httpServer, {
  path: '/socket.io',
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  pingTimeout: 60000,
  pingInterval: 25000,
})

// onlineUsers: Map<socketId, { id, username, avatar, socketId }>
const onlineUsers = new Map()
// userChannels: Map<socketId, Set<channelId>>
const userChannels = new Map()
// typingUsers: Map<channelId, Set<socketId>>
const typingUsers = new Map()
// userSocketsMap: Map<userId, Set<socketId>> — tracks ALL sockets per user
const userSocketsMap = new Map()

const generateId = () => Math.random().toString(36).substr(2, 9) + Date.now().toString(36)

// Helper: broadcast deduplicated online users
function broadcastOnlineUsers() {
  const seen = new Set()
  const uniqueUsers = []
  for (const u of onlineUsers.values()) {
    if (!seen.has(u.id)) {
      seen.add(u.id)
      uniqueUsers.push({ id: u.id, username: u.username, avatar: u.avatar })
    }
  }
  io.emit('online-users', uniqueUsers)
}

io.on('connection', (socket) => {
  console.log(`[Chat] User connected: ${socket.id}`)

  socket.on('auth', (data) => {
    const { userId, username, avatar } = data

    // Track this socket for the user (supports multiple devices/tabs)
    if (!userSocketsMap.has(userId)) {
      userSocketsMap.set(userId, new Set())
    }
    userSocketsMap.get(userId).add(socket.id)

    // Store user info keyed by socketId
    onlineUsers.set(socket.id, {
      id: userId,
      username,
      avatar,
      socketId: socket.id
    })

    userChannels.set(socket.id, new Set())

    // Broadcast deduplicated online users
    broadcastOnlineUsers()

    console.log(`[Chat] ${username} authenticated (socket: ${socket.id}), total sockets for user: ${userSocketsMap.get(userId).size}, online sockets: ${onlineUsers.size}`)
  })

  socket.on('join-channel', (data) => {
    const { channelId } = data
    const user = onlineUsers.get(socket.id)
    if (!user) return

    socket.join(channelId)
    userChannels.get(socket.id)?.add(channelId)

    socket.to(channelId).emit('user-joined-channel', {
      channelId,
      user: { id: user.id, username: user.username, avatar: user.avatar }
    })

    console.log(`[Chat] ${user.username} joined channel: ${channelId}`)
  })

  socket.on('leave-channel', (data) => {
    const { channelId } = data
    const user = onlineUsers.get(socket.id)
    if (!user) return

    socket.leave(channelId)
    userChannels.get(socket.id)?.delete(channelId)
    typingUsers.get(channelId)?.delete(socket.id)

    socket.to(channelId).emit('user-left-channel', {
      channelId,
      user: { id: user.id, username: user.username, avatar: user.avatar }
    })
  })

  socket.on('send-message', (data) => {
    const { channelId, content, userId, username, avatar, type = 'text' } = data
    const user = onlineUsers.get(socket.id)
    if (!user) return

    typingUsers.get(channelId)?.delete(socket.id)
    io.to(channelId).emit('typing-users', {
      channelId,
      users: Array.from(typingUsers.get(channelId) || []).map(sid => {
        const u = onlineUsers.get(sid)
        return u ? { id: u.id, username: u.username } : null
      }).filter(Boolean)
    })

    const message = {
      id: generateId(),
      content,
      type,
      userId,
      username,
      avatar,
      channelId,
      createdAt: new Date().toISOString()
    }

    // Use socket.to() to avoid sending back to the sender
    // The sender already has the message via optimistic local add
    socket.to(channelId).emit('new-message', message)
  })

  socket.on('typing', (data) => {
    const { channelId, isTyping } = data
    const user = onlineUsers.get(socket.id)
    if (!user) return

    if (!typingUsers.has(channelId)) {
      typingUsers.set(channelId, new Set())
    }

    if (isTyping) {
      typingUsers.get(channelId)?.add(socket.id)
    } else {
      typingUsers.get(channelId)?.delete(socket.id)
    }

    socket.to(channelId).emit('typing-users', {
      channelId,
      users: Array.from(typingUsers.get(channelId) || []).map(sid => {
        const u = onlineUsers.get(sid)
        return u ? { id: u.id, username: u.username } : null
      }).filter(Boolean)
    })
  })

  // WebRTC signaling
  socket.on('call-user', (data) => {
    const { targetUserId, callerId, callerName, callerAvatar, callType, offer } = data
    const sockets = userSocketsMap.get(targetUserId)

    if (!sockets || sockets.size === 0) {
      socket.emit('call-failed', { reason: 'User is offline' })
      return
    }

    console.log(`[Call] ${callerName} calling ${targetUserId} (${callType})`)
    // Send to all sockets of the target user
    for (const sid of sockets) {
      io.to(sid).emit('incoming-call', {
        callerId,
        callerName,
        callerAvatar,
        callType,
        offer,
      })
    }
  })

  socket.on('answer-call', (data) => {
    const { callerId, answer } = data
    const sockets = userSocketsMap.get(callerId)

    if (!sockets || sockets.size === 0) {
      socket.emit('call-failed', { reason: 'Caller is offline' })
      return
    }

    console.log(`[Call] Call answered, sending answer to ${callerId}`)
    for (const sid of sockets) {
      io.to(sid).emit('call-answered', { answer })
    }
  })

  socket.on('reject-call', (data) => {
    const { callerId } = data
    const sockets = userSocketsMap.get(callerId)

    if (sockets) {
      console.log(`[Call] Call rejected by callee, notifying ${callerId}`)
      for (const sid of sockets) {
        io.to(sid).emit('call-rejected')
      }
    }
  })

  socket.on('end-call', (data) => {
    const { targetUserId } = data
    const sockets = userSocketsMap.get(targetUserId)

    if (sockets) {
      console.log(`[Call] Call ended, notifying ${targetUserId}`)
      for (const sid of sockets) {
        io.to(sid).emit('call-ended')
      }
    }
  })

  socket.on('ice-candidate', (data) => {
    const { targetUserId, candidate } = data
    const sockets = userSocketsMap.get(targetUserId)

    if (sockets) {
      for (const sid of sockets) {
        io.to(sid).emit('ice-candidate', { candidate })
      }
    }
  })

  socket.on('disconnect', () => {
    const user = onlineUsers.get(socket.id)
    if (user) {
      const channels = userChannels.get(socket.id)
      if (channels) {
        channels.forEach(channelId => {
          typingUsers.get(channelId)?.delete(socket.id)
          socket.to(channelId).emit('user-left-channel', {
            channelId,
            user: { id: user.id, username: user.username, avatar: user.avatar }
          })
        })
      }

      // Remove this specific socket
      onlineUsers.delete(socket.id)
      userChannels.delete(socket.id)

      // Remove socket from user's socket set
      const sockets = userSocketsMap.get(user.id)
      if (sockets) {
        sockets.delete(socket.id)
        // Only fully remove user if they have no more active sockets
        if (sockets.size === 0) {
          userSocketsMap.delete(user.id)
        }
      }

      // Broadcast updated online users
      broadcastOnlineUsers()

      console.log(`[Chat] ${user.username} disconnected (socket: ${socket.id}), remaining sockets: ${sockets ? sockets.size : 0}`)
    }
  })

  socket.on('error', (error) => {
    console.error(`[Chat] Socket error (${socket.id}):`, error)
  })
})

const PORT = process.env.PORT || 3099
httpServer.listen(PORT, () => {
  console.log(`[Chat] Socket.io server running on port ${PORT}`)
})

process.on('SIGTERM', () => {
  console.log('[Chat] Received SIGTERM, shutting down...')
  httpServer.close(() => {
    console.log('[Chat] Server closed')
    process.exit(0)
  })
})

process.on('SIGINT', () => {
  console.log('[Chat] Received SIGINT, shutting down...')
  httpServer.close(() => {
    console.log('[Chat] Server closed')
    process.exit(0)
  })
})
