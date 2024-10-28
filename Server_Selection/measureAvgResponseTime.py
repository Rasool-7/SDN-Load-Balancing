import requests
import time
import matplotlib.pyplot as plt

service_ip = "10.0.1.1"  # Replace with the IP your load balancer uses
url = f"http://{service_ip}/"

# List of different packet counts to test
request_counts = [100, 500, 1000, 5000]

# To store the average response time for each packet count
average_response_times = []

for count in request_counts:
    latencies = []  # Initialize latencies for the current packet count
    
    for i in range(count):  # Make 'count' number of requests
        start_time = time.time()
        response = requests.get(url)
        end_time = time.time()
        
        latencies.append(end_time - start_time)

       # print(f"Request {i+1}/{count} for packet count {count}: {end_time - start_time:.4f} seconds")
    
    # Calculate average response time for the current packet count
    average_response_time = sum(latencies) / len(latencies)
    average_response_times.append(average_response_time)
    
    print(f"Average response time for {count} requests: {average_response_time:.4f} seconds")

# Plotting the average response time for different packet counts
plt.plot(request_counts, average_response_times, marker='o')
plt.title('Average HTTP Response Time vs Requests Count')
plt.xlabel('Number of Packets')
plt.ylabel('Average Response Time (seconds)')
plt.grid(True)
plt.show()

