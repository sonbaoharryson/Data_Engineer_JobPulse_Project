#!/bin/sh

echo "Starting Ollama server..."
ollama serve &

echo "Waiting for Ollama..."
until ollama list > /dev/null 2>&1; do
  sleep 2
done

echo "Pulling models..."
ollama pull mxbai-embed-large:latest
ollama pull gemma3:1b

wait