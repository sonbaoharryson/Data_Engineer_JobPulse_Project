// Initialize MongoDB chat_db database
db = db.getSiblingDB('chat_db');

// Create chat_messages collection with validation schema
db.createCollection('chat_messages', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['user_id', 'role', 'message', 'timestamp'],
            properties: {
                user_id: {
                    bsonType: 'string',
                    description: 'User ID - required'
                },
                role: {
                    bsonType: 'string',
                    description: 'Message role (user, assistant, system) - required'
                },
                message: {
                    bsonType: 'string',
                    description: 'Message content - required'
                },
                timestamp: {
                    bsonType: 'date',
                    description: 'Timestamp when message was added to MongoDB - required'
                }
            }
        }
    }
});

// Create indexes
db.chat_messages.createIndex({ user_id: 1 });
db.chat_messages.createIndex({ timestamp: 1 });
db.chat_messages.createIndex({ user_id: 1, timestamp: 1 });

print('MongoDB initialization completed. Collection chat_messages created with proper schema and indexes.');
