# Mini HDFS Simulation - Exploring Distributed Data Storage

A hands-on implementation of a miniature Hadoop Distributed File System (HDFS) that demonstrates core distributed systems concepts including file chunking, replication, metadata management, and fault tolerance.

## Features
- **File Chunking**: Automatic splitting of files into 2MB chunks
- **Replication**: Chunk replication across multiple Datanodes for fault tolerance
- **Namenode**: Central metadata store and coordinator
- **Dual Datanodes**: Distributed storage with heartbeat monitoring
- **Web Dashboard**: Real-time visualization of chunk distribution and system health
- **End-to-End Integrity**: Safe file upload/download with zero data corruption

## Architecture
- **Client UI**: User-friendly interface for file operations
- **Namenode**: Handles chunking, metadata, replication coordination
- **Datanodes (2)**: Store chunks and maintain system health via heartbeats

## Tech Stack
- Python (networking, threading, core logic)
- TCP Sockets (inter-node communication)
- Multi-threading (concurrency and fault-tolerance)
- Flask/FastAPI (REST API and dashboard)
- HTML/JavaScript (interactive frontend)

## Team Members & Roles

**Mohammed Aahil Parson** - Namenode (Controller)
- Core control layer for chunking, metadata, replication, and heartbeat management

**Mohammed Mir Fazlai Ali** - Client (Frontend)
- File upload/download interface and communication with Namenode

**Mohit Kumar** - Datanode:1 (Storage Node B)
- Secondary datanode for replication & redundancy

**Mohammed Shehzaad Khan** - Datanode:0 (Storage Node A)
- Primary storage node for receiving, storing, and serving file chunks