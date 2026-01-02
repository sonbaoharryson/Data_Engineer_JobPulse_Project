import requests

# Test the endpoints
try:
    # Test TopCV endpoint
    print("Testing TopCV endpoint...")
    topcv_response = requests.get('http://localhost:5000/topcv/jobs')
    print(f"Status Code: {topcv_response.status_code}")
    if topcv_response.status_code == 200:
        print(topcv_response.json())
    else:
        print(f"Error: {topcv_response.text}")

    # Test ITViec endpoint
    print("\nTesting ITViec endpoint...")
    itviec_response = requests.get('http://localhost:5000/itviec/jobs')
    print(f"Status Code: {itviec_response.status_code}")
    if itviec_response.status_code == 200:
        print(itviec_response.json())
    else:
        print(f"Error: {itviec_response.text}")

except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")