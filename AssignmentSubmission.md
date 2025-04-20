# Mattermost Server Improvements Documentation

## Approach
The improvements focused on enhancing the WebSocket event handling and cluster communication system in Mattermost. The key areas addressed were:

1. WebSocket event broadcasting optimization
2. Reliable message delivery in clustered environments
3. Performance improvements in event processing
4. Enhanced error handling and logging

My thought process was:
1. What is this repo? How are things structured? What are the main components that I need to understand? 
Here, Naman Sir and one of my peer helped me point to the right direction.
Understood the flow of the code. 
- starts at server/channels/app/server.go
- has mainly three functions: Start(), Shutdown(), and init_jobs()
- the Start() function is the main function that starts the server.
- it takes a platformService object and proceeds.

- platformService is the place where the app is configured. 
- it is where all the configuration is done, and license is checked.
- before, as the free version was failing the license check, it was falling back to single server implementation.
- platformService has a variable called cluster, which is the cluster implementation. if it is enabled, it will initialize the cluster implementation.


2. What are the main things that I need to understand?
- How does pubsub work?
> The pubsub (publish-subscribe) system in Mattermost is a messaging pattern where messages are sent by publishers and received by subscribers.  It allows for decoupled communication between different parts of the system. In Mattermost, this is used to broadcast events across different components, ensuring that all interested parties receive updates without direct connections between them.

- How does the WebSocket event system work?
> The WebSocket event system in Mattermost is responsible for handling real-time communication between the server and clients. It supports various event types, such as typing notifications, message postings, and channel updates.

- How does the Redis communication work?
> Redis is used for caching and storing data in the cluster. It is used to store the cluster information and the state of the cluster. If multiple servers are running, they will all connect to the same Redis instance.

- How does the cluster implementation work?
> The cluster implementation in Mattermost is responsible for managing the cluster of servers. It allows for the servers to communicate with each other and for the users to be distributed across multiple servers.

- How does the message delivery system work?
> The message delivery system in Mattermost is responsible for delivering messages to the correct users. It is used to deliver messages to the clients.

- How does the leader election work?
> The leader election mechanism in Mattermost is used to determine which server should be the leader of the cluster. It is used to ensure that only one server is the leader at a time.

- How does the message batching work?
> The message batching mechanism in Mattermost is used to improve the performance of the server. It is used to batch messages together and send them in a single request to the clients.

- How does the error handling and logging work?
> The error handling and logging mechanism in Mattermost is used to handle errors and log them. It is used to log the errors and the stack trace of the errors.

- How does the health monitoring and synchronization work?
> The health monitoring and synchronization mechanism in Mattermost is used to monitor the health of the servers and to synchronize the servers.

3. How did I implement the changes?
- changed the config to enable the cluster.
- changed the config to use the Redis instance.
- wrote a new cluster implementation in server/channels/app/platform/community_cluster.go which we will use for the cluster implementation.
- in server/channels/app/platform/service.go, I force enabled Redis and called the new cluster implementation.
- in public/model/websocket_cluster.go, I added the new cluster implementation.
- in server/channels/app/server.go, I bypassed the license check and force enabled the cluster.





## System Architecture

### Core Components
1. **WebSocket Event System**
   - Handles real-time communication between server and clients
   - Supports various event types (typing, posted, channel updates, etc.)
   - Implements efficient event broadcasting

2. **Cluster Communication**
   - Redis-based cluster implementation
   - Leader election mechanism
   - Reliable message delivery system
   - Health monitoring and synchronization

3. **Event Processing Pipeline**
   - Message buffering and batching
   - Hook-based event processing
   - Efficient JSON serialization/deserialization

## Key Code Changes

### 1. WebSocket Event Handling
- Implemented precomputed JSON for repeated event broadcasts
- Added support for broadcast hooks with arguments
- Enhanced event copying and deep copying mechanisms
- Improved event validation and type safety

### 2. Cluster Communication
- Added reliable message delivery system
- Implemented message retry queue
- Enhanced leader election with backoff mechanism
- Added health monitoring and synchronization

### 3. Performance Optimizations
- Implemented message batching for better throughput
- Added connection pooling for Redis operations
- Optimized JSON serialization with precomputed buffers
- Enhanced caching mechanisms

### 4. Error Handling and Logging
- Added comprehensive error types
- Implemented structured logging
- Added health check metrics
- Enhanced error recovery mechanisms

## Challenges and Solutions

### 1. Message Reliability
**Challenge**: Ensuring message delivery in distributed environment
**Solution**: 
- Implemented reliable message delivery system
- Added retry queue with exponential backoff
- Added message deduplication

### 2. Performance
**Challenge**: Handling high volume of WebSocket events
**Solution**:
- Implemented message batching
- Added precomputed JSON for repeated events
- Optimized Redis operations with pipelining

### 3. Cluster Coordination
**Challenge**: Maintaining cluster consistency
**Solution**:
- Enhanced leader election mechanism
- Added health monitoring
- Implemented synchronization checks

### 4. Error Recovery
**Challenge**: Handling network failures and node disconnections
**Solution**:
- Added comprehensive error handling
- Implemented automatic retry mechanisms
- Enhanced logging for debugging

## Positive Aspects

1. **Scalability**
   - Efficient message batching
   - Optimized Redis operations
   - Connection pooling

2. **Reliability**
   - Robust error handling
   - Message retry mechanisms
   - Health monitoring

3. **Performance**
   - Precomputed JSON for repeated events
   - Efficient message processing
   - Optimized cluster communication

4. **Maintainability**
   - Clear code structure
   - Comprehensive logging
   - Well-documented interfaces

5. **Extensibility**
   - Hook-based event processing
   - Flexible broadcast system
   - Modular cluster implementation 

## Testing and Validation

- Spawned two servers manually using this script:
```
export GO_BUILD_TAGS="oss_cluster"
export MM_FILESETTINGS_DIRECTORY="/tmp/mmdata2"
export MM_SERVICESETTINGS_LISTENADDRESS=":8066"
export MM_SQLSETTINGS_DATASOURCE="postgres://mmuser:mostest@localhost/mattermost_test?sslmode=disable"
export MM_CACHESETTINGS_REDISADDRESS="localhost:6379"
export MM_CLUSTERSETTINGS_ENABLE="true"

make run-server
```
Changed the ports for the other server

- Manually sent messages between the two servers, and verified they were received by the other server. It was real-time and worked as expected.

- Manually changed the status of one user on one server, and verified it was updated on the other server.
