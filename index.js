// This file is used by Railway (Node.js) and local dev (Bun)
const { createServer } = require('http')
const { Server } = require('socket.io')

const httpServer = createServer()
const io = new Server(httpServer, {
  path: '/',
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  pingTimeout: 60000,
  pingInterval: 25000,
})

const onlineUsers = new Map()
const userChannels = new Map()
const typingUsers = new Map()
const userSocketMap = new Map()

const generateId = () => Math.random().toString(36).substr(2, 9) + Date.now().toString(36)

io.on('connection', (socket) => {
  console.log(`[Chat] User connected: ${socket.id}`)

  socket.on('auth', (data) => {
    const { userId, username, avatar } = data

    const onlineUser = {
      id: userId,
      username,
      avatar,
      socketId: socket.id
    }

    onlineUsers.set(socket.id, onlineUser)
    userChannels.set(socket.id, new Set())
    userSocketMap.set(userId, socket.id)

    io.emit('online-users', Array.from(onlineUsers.values()).map(u => ({
      id: u.id,
      username: u.username,
      avatar: u.avatar
    })))

    console.log(`[Chat] ${username} authenticated, online: ${onlineUsers.size}`)
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

    io.to(channelId).emit('new-message', message)
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
    const targetSocketId = userSocketMap.get(targetUserId)

    if (!targetSocketId) {
      socket.emit('call-failed', { reason: 'User is offline' })
      return
    }

    console.log(`[Call] ${callerName} calling ${targetUserId} (${callType})`)
    io.to(targetSocketId).emit('incoming-call', {
      callerId,
      callerName,
      callerAvatar,
      callType,
      offer,
    })
  })

  socket.on('answer-call', (data) => {
    const { callerId, answer } = data
    const callerSocketId = userSocketMap.get(callerId)

    if (!callerSocketId) {
      socket.emit('call-failed', { reason: 'Caller is offline' })
      return
    }

    console.log(`[Call] Call answered, sending answer to ${callerId}`)
    io.to(callerSocketId).emit('call-answered', { answer })
  })

  socket.on('reject-call', (data) => {
    const { callerId } = data
    const callerSocketId = userSocketMap.get(callerId)

    if (callerSocketId) {
      console.log(`[Call] Call rejected by callee, notifying ${callerId}`)
      io.to(callerSocketId).emit('call-rejected')
    }
  })

  socket.on('end-call', (data) => {
    const { targetUserId } = data
    const targetSocketId = userSocketMap.get(targetUserId)

    if (targetSocketId) {
      console.log(`[Call] Call ended, notifying ${targetUserId}`)
      io.to(targetSocketId).emit('call-ended')
    }
  })

  socket.on('ice-candidate', (data) => {
    const { targetUserId, candidate } = data
    const targetSocketId = userSocketMap.get(targetUserId)

    if (targetSocketId) {
      io.to(targetSocketId).emit('ice-candidate', { candidate })
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

      onlineUsers.delete(socket.id)
      userChannels.delete(socket.id)
      userSocketMap.delete(user.id)

      io.emit('online-users', Array.from(onlineUsers.values()).map(u => ({
        id: u.id,
        username: u.username,
        avatar: u.avatar
      })))

      console.log(`[Chat] ${user.username} disconnected, online: ${onlineUsers.size}`)
    }
  })

  socket.on('error', (error) => {
    console.error(`[Chat] Socket error (${socket.id}):`, error)
  })
})

const PORT = process.env.PORT || 3003
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
