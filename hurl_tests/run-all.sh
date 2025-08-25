#!/bin/bash

# Run all hurl tests independently
# Usage: ./tests/run-all.sh

echo "Running API Tests..."
echo "==================="

echo -e "\n1. Testing Health Endpoint..."
hurl --test tests/health.hurl

echo -e "\n2. Testing Root Endpoint..."
hurl --test tests/root.hurl

echo -e "\n3. Testing Wellbeing Calculation..."
hurl --test tests/wellbeing.hurl

echo -e "\n4. Testing Bus Network Calculation..."
hurl --test tests/bus-network.hurl

echo -e "\n5. Testing Road Network Calculation..."
hurl --test tests/road-network.hurl

# echo -e "\n6. Testing Asset Network Calculation..."
# hurl --test tests/asset-network.hurl

echo -e "\n7. Testing Error Cases..."
hurl --test tests/error-cases.hurl

echo -e "\n==================="
echo "All tests completed!"
