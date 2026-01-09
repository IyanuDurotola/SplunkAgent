# SplunkProcessor - AI-Powered Bug Investigation System

## Overview

SplunkProcessor is an AI-powered bug investigation and root cause analysis system that automates the investigation of technical issues by intelligently querying Splunk logs and metrics, analyzing results, and providing evidence-based root cause explanations.

## Purpose

This system automates the investigation of technical issues by:

1. **Accepting natural language questions** about system problems (e.g., "Why is the payment service failing?")

2. **Extracting intent** from the question:
   - Entities (services, systems, components)
   - Symptom keywords (errors, issues)
   - Time references

3. **Generating investigation hypotheses** 
   - Using AI to identify potential root causes
   - Using a catalog of related services to find possible errors

4. **Retrieving historical context** from a vector database (PostgreSQL with pgvector) of past incidents and solutions

5. **Executing multi-step investigation**:
   - Generating Splunk Query Language (SPL) queries for each hypothesis
   - Executing queries against Splunk logs/metrics
   - Analyzing results to find patterns and correlations

6. **Extracting evidence and computing confidence scores** to determine the most likely root cause

7. **Generating a final answer** with:
   - Root cause explanation
   - Evidence supporting the conclusion
   - Confidence score
   - Investigation steps taken

8. **Storing the investigation** in memory for future reference (RAG/vector search)

## Key Technologies

- **Amazon Bedrock**: Claude for LLM operations, Titan for embeddings
- **PostgreSQL with pgvector**: Vector database for RAG (Retrieval-Augmented Generation)
- **Splunk**: Log and metric analysis platform
- **FastAPI**: REST API gateway

## Architecture

```mermaid

sequenceDiagram
    autonumber

    participant User as User / UI
    participant Gateway as AI Query Gateway<br/>(FastAPI / Auth / RBAC)
    participant Orchestrator as Investigation Orchestrator<br/>(Planning & Control)
    participant Memory as Knowledge Memory<br/>(RAG / Vector DB)
    participant QueryGen as Splunk Query Generator<br/>(LLM + Guardrails)
    participant SplunkAPI as Splunk Integration<br/>(REST / HEC API)
    participant Splunk as Splunk Logs / Metrics
    participant Analyzer as Result Analyzer & RCA Engine
    participant Evidence as Evidence & Confidence Engine
    participant Answer as Final Answer Generator

    User->>Gateway: Natural language question
    Gateway->>Orchestrator: Authenticated request

    Orchestrator->>Orchestrator: Extract intent & time window
    Orchestrator->>Orchestrator: Generate investigation hypotheses

    Orchestrator->>Memory: Retrieve past incidents\n& known failure patterns
    Memory-->>Orchestrator: Relevant historical context

    loop Multi-step investigation
        Orchestrator->>QueryGen: Request SPL for next hypothesis
        QueryGen->>QueryGen: Validate & constrain query
        QueryGen->>SplunkAPI: Execute SPL query
        SplunkAPI->>Splunk: Query logs/metrics
        Splunk-->>SplunkAPI: Query results
        SplunkAPI-->>QueryGen: Structured results
        QueryGen-->>Orchestrator: Query results
        Orchestrator->>Analyzer: Analyze results
        Analyzer-->>Orchestrator: Findings & correlations
    end

    Orchestrator->>Evidence: Extract evidence\n& compute confidence
    Evidence-->>Orchestrator: Evidence set & confidence score

    Orchestrator->>Answer: Generate grounded explanation
    Answer-->>Gateway: Final answer with evidence & confidence
    Gateway-->>User: Response


# Testing Guide for SplunkProcessor
   ```bash
    curl -X POST "http://localhost:8082/api/v1/query" \
    -H "Content-Type: application/json" \
    -d '{
        "question": "Why is the system slow?",
        "time_window": "24h"
    }'
   ```
