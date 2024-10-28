import requests
import time
import matplotlib.pyplot as plt

# Configuration
service_ip = "10.0.1.1"  # Replace with the IP your load balancer uses
url = f"http://{service_ip}/"
request_counts = [100,500,1000,5000]  
throughputs = []  # List to store throughput values 

for num_requests in request_counts:
    total_data_received = 0  
    
    # Measure throughput
    start_time = time.time()
    for _ in range(num_requests):
        response = requests.get(url)
        total_data_received += len(response.content)  # Add size of each response
    end_time = time.time()

    # Calculate throughput in Mbps
    total_time = end_time - start_time
    throughput_mbps = (total_data_received * 8) / (total_time * 1e6)  # Convert bytes to megabits
    
    # Store throughput value
    throughputs.append(throughput_mbps)
    
    print(f"Packet count: {num_requests}")
    print(f"Total data received: {total_data_received} bytes")
    print(f"Total time taken: {total_time} seconds")
    print(f"Throughput: {throughput_mbps} Mbps")
    print()

# Plot the results
plt.figure(figsize=(10, 6))
plt.plot(request_counts, throughputs, marker='o')
plt.xlabel('Number of Packets')
plt.ylabel('Throughput (Mbps)')
plt.title('Throughput vs. Number of Requests')
plt.grid(True)
plt.show()


